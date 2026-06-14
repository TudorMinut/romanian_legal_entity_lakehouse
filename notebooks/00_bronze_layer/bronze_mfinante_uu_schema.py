# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Initialize catalog and schema
from pyspark.sql import functions as F
from functools import reduce
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

mfinante_sf_path = f"{bucket}/raw_v2/mfinante/situatii_financiare_uu/"

display(dbutils.fs.ls(mfinante_sf_path))

# COMMAND ----------

# DBTITLE 1,List all files in source directory
def list_all_files(path):
    all_files = []

    def recurse(p):
        for item in dbutils.fs.ls(p):
            if item.isDir():
                recurse(item.path)
            else:
                all_files.append(item.path)

    recurse(path)
    return all_files


all_files = list_all_files(mfinante_sf_path)

print("Total files found:", len(all_files))

for f in sorted(all_files):
    print(f)

# COMMAND ----------

# DBTITLE 1,Identify CSV schema files by year
def extract_year(path):
    match = re.search(r"web_uu_an(\d{4})\.(txt|csv)$", path, re.IGNORECASE)
    return int(match.group(1)) if match else None


web_uu_csv_files = [
    f for f in all_files
    if re.search(r"web_uu_an\d{4}\.csv$", f, re.IGNORECASE)
]

csv_by_year = {
    extract_year(f): f
    for f in web_uu_csv_files
    if extract_year(f) is not None
}

available_years = sorted(csv_by_year.keys())

print("CSV years:", available_years)

# COMMAND ----------

# DBTITLE 1,Load schema metadata from CSV files
# Get already-processed files (serverless-compatible)
processed_rows = spark.table("company_ro.bronze.metadata_files_processed").filter(F.col("table_name") == "mfinante_uu_schema_raw").select("file_path").collect()
processed_files = [row["file_path"] for row in processed_rows]

# Filter to only NEW files
new_files_by_year = {year: path for year, path in csv_by_year.items() if path not in processed_files}

if not new_files_by_year:
    print("\n✓ No new files to process.")
    dbutils.notebook.exit("No new data")

print(f"New files to process: {len(new_files_by_year)}")

schema_dfs = []
file_metadata = []

for year in sorted(new_files_by_year.keys()):
    csv_path = new_files_by_year[year]

    print(f"\nLoading schema file for year {year}")
    print(csv_path)

    schema_df = (
        spark.read.text(csv_path)
        .select(
            "*",
            F.lit(year).alias("_source_year"),
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file")
        )
    )

    row_count = schema_df.count()
    file_size = dbutils.fs.ls(csv_path)[0].size
    file_metadata.append((csv_path, "mfinante_uu_schema_raw", row_count, file_size))

    schema_dfs.append(schema_df)
    print(f"  Loaded {row_count} rows")

mfinante_uu_schema_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), schema_dfs)

display(mfinante_uu_schema_all.groupBy("_source_year").count().orderBy("_source_year"))

# COMMAND ----------

# DBTITLE 1,Preview schema metadata
display(mfinante_uu_schema_all.orderBy("_source_year").limit(100))

print("Rows:", mfinante_uu_schema_all.count())
print("Columns:", mfinante_uu_schema_all.columns)

# COMMAND ----------

# DBTITLE 1,Write schema metadata to bronze table
(
    mfinante_uu_schema_all
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("company_ro.bronze.mfinante_uu_schema_raw")
)

print(f"✓ Appended {mfinante_uu_schema_all.count():,} rows")

# Record in checkpoint
checkpoint_df = (
    spark.createDataFrame(
        [(path, table_name, rows, size) for path, table_name, rows, size in file_metadata],
        ["file_path", "table_name", "row_count", "file_size"]
    )
    .withColumn("ingested_at", F.current_timestamp())
)
checkpoint_df.write.format("delta").mode("append").saveAsTable("company_ro.bronze.metadata_files_processed")
print(f"✓ Recorded {len(file_metadata)} files")

# COMMAND ----------

# DBTITLE 1,Verify schema ingestion by year
display(spark.sql("""
SELECT
  _source_year,
  COUNT(*) AS schema_rows,
  COUNT(DISTINCT _source_file) AS source_files
FROM company_ro.bronze.mfinante_uu_schema_raw
GROUP BY _source_year
ORDER BY _source_year
"""))
