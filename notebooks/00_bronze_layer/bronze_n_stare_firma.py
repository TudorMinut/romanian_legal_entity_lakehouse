# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Initialize catalog, schema, and checkpoint table
from pyspark.sql import functions as F
import re

spark.sql("CREATE CATALOG IF NOT EXISTS company_ro")
spark.sql("CREATE SCHEMA IF NOT EXISTS company_ro.bronze")
spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.bronze.metadata_files_processed (
  file_path STRING, table_name STRING, ingested_at TIMESTAMP, row_count BIGINT, file_size BIGINT
) USING DELTA
""")
print("✓ Checkpoint ready")

# COMMAND ----------

# DBTITLE 1,Define source data path
bucket = "s3://ro-company-lake"
onrc_nomenclatoare_path = f"{bucket}/raw_v2/onrc/nomenclatoare/"

display(dbutils.fs.ls(onrc_nomenclatoare_path))

# COMMAND ----------

# DBTITLE 1,Load ONLY new snapshot dates (serverless-compatible)
all_snapshots = dbutils.fs.ls(onrc_nomenclatoare_path)
snapshot_dates = [re.search(r"snapshot_date=(\d{4}-\d{2}-\d{2})", s.path).group(1) for s in all_snapshots if "snapshot_date=" in s.path]

processed_rows = spark.table("company_ro.bronze.metadata_files_processed").filter(F.col("table_name") == "n_stare_firma_raw").select("file_path").collect()
processed_snapshots = [row["file_path"] for row in processed_rows]
processed_dates = [re.search(r"snapshot_date=(\d{4}-\d{2}-\d{2})", path).group(1) for path in processed_snapshots if "snapshot_date=" in path]

new_snapshots = [d for d in snapshot_dates if d not in processed_dates]

if not new_snapshots:
    print("\n✓ No new snapshots to process.")
    dbutils.notebook.exit("No new data")

print(f"New snapshots: {sorted(new_snapshots)}")

n_stare_firma = spark.read.option("header", True).option("inferSchema", False).option("sep", "^").option("encoding", "UTF-8").csv([f"{onrc_nomenclatoare_path}snapshot_date={date}/package=*/n_stare_firma.csv" for date in new_snapshots])

display(n_stare_firma.limit(20))
row_count = n_stare_firma.count()
print("New rows:", row_count)

# COMMAND ----------

# DBTITLE 1,Append new data and update checkpoint
(
    n_stare_firma
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("company_ro.bronze.n_stare_firma_raw")
)

print(f"✓ Appended {row_count:,} rows")

checkpoint_records = [(f"{onrc_nomenclatoare_path}snapshot_date={date}/package=*/n_stare_firma.csv", "n_stare_firma_raw", row_count // len(new_snapshots), 0) for date in new_snapshots]
checkpoint_df = (
    spark.createDataFrame(
        [(path, table_name, rows, size) for path, table_name, rows, size in checkpoint_records],
        ["file_path", "table_name", "row_count", "file_size"]
    )
    .withColumn("ingested_at", F.current_timestamp())
)
checkpoint_df.write.format("delta").mode("append").saveAsTable("company_ro.bronze.metadata_files_processed")
print(f"✓ Recorded {len(new_snapshots)} snapshots")

# COMMAND ----------

# DBTITLE 1,Verify bronze table
display(spark.sql("""
SELECT COUNT(*) AS rows
FROM company_ro.bronze.n_stare_firma_raw
"""))
