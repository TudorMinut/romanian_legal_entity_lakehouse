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

# Create checkpoint table to track processed files
spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.bronze.metadata_files_processed (
  file_path STRING,
  table_name STRING,
  ingested_at TIMESTAMP,
  row_count BIGINT,
  file_size BIGINT
)
USING DELTA
""")

print("✓ Checkpoint table ready: company_ro.bronze.metadata_files_processed")

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

# DBTITLE 1,Identify TXT files by year
def extract_year(path):
    match = re.search(r"web_uu_an(\d{4})\.(txt|csv)$", path, re.IGNORECASE)
    return int(match.group(1)) if match else None


web_uu_txt_files = [
    f for f in all_files
    if re.search(r"web_uu_an\d{4}\.txt$", f, re.IGNORECASE)
]

txt_by_year = {
    extract_year(f): f
    for f in web_uu_txt_files
    if extract_year(f) is not None
}

available_years = sorted(txt_by_year.keys())

print("TXT years:", available_years)

# COMMAND ----------

# DBTITLE 1,Define separator detection function
candidate_separators = ["^", ";", "|", "\t", ","]


def detect_separator(path):
    preview = spark.read.text(path)
    first_line = preview.limit(1).collect()[0]["value"]

    counts = {
        sep: first_line.count(sep)
        for sep in candidate_separators
    }

    detected = max(counts, key=counts.get)

    print("Path:", path)
    print("Detected separator:", repr(detected))
    print("Counts:", counts)

    return detected

# COMMAND ----------

# DBTITLE 1,Load financial data from TXT files
# Get already-processed files from checkpoint (serverless-compatible)
processed_rows = (
    spark.table("company_ro.bronze.metadata_files_processed")
    .filter(F.col("table_name") == "mfinante_uu_raw")
    .select("file_path")
    .collect()
)
processed_files = [row["file_path"] for row in processed_rows]

print(f"Already processed: {len(processed_files)} files")

# Filter to only NEW files
new_files_by_year = {
    year: path
    for year, path in txt_by_year.items()
    if path not in processed_files
}

if not new_files_by_year:
    print("\n✓ No new files to process. All files already ingested.")
    dbutils.notebook.exit("No new data")

print(f"\nNew files to process: {len(new_files_by_year)}")
for year in sorted(new_files_by_year.keys()):
    print(f"  Year {year}: {new_files_by_year[year]}")

# Load ONLY new files
raw_dfs = []
file_metadata = []

for year in sorted(new_files_by_year.keys()):
    txt_path = new_files_by_year[year]

    print(f"\nLoading financial data for year {year}")
    print(txt_path)

    data_sep = detect_separator(txt_path)

    # Use .select() instead of chained .withColumn() for Spark Connect performance
    raw_df = (
        spark.read
        .option("header", False)
        .option("inferSchema", False)
        .option("sep", data_sep)
        .option("encoding", "UTF-8")
        .csv(txt_path)
        .select(
            "*",
            F.lit(year).alias("_source_year"),
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file")
        )
    )

    row_count = raw_df.count()
    file_size = dbutils.fs.ls(txt_path)[0].size

    file_metadata.append((txt_path, "mfinante_uu_raw", row_count, file_size))

    raw_dfs.append(raw_df)
    print(f"  Loaded {row_count:,} rows")

mfinante_uu_all = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    raw_dfs
)

display(
    mfinante_uu_all
    .groupBy("_source_year")
    .count()
    .orderBy("_source_year")
)

# COMMAND ----------

# DBTITLE 1,Preview loaded financial data
display(mfinante_uu_all.limit(20))

print("Rows:", mfinante_uu_all.count())
print("Columns:", mfinante_uu_all.columns)
print("Number of columns:", len(mfinante_uu_all.columns))

# COMMAND ----------

# DBTITLE 1,Write financial data to bronze table
# Append new data (not overwrite)
(
    mfinante_uu_all
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("company_ro.bronze.mfinante_uu_raw")
)

print(f"✓ Appended {mfinante_uu_all.count():,} rows to company_ro.bronze.mfinante_uu_raw")

# Record processed files in checkpoint
checkpoint_df = (
    spark.createDataFrame(
        [(path, table_name, row_count, file_size)
         for path, table_name, row_count, file_size in file_metadata],
        ["file_path", "table_name", "row_count", "file_size"]
    )
    .withColumn("ingested_at", F.current_timestamp())
)

(
    checkpoint_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("company_ro.bronze.metadata_files_processed")
)

print(f"✓ Recorded {len(file_metadata)} new files in checkpoint table")

# COMMAND ----------

# DBTITLE 1,Verify data ingestion by year
display(spark.sql("""
SELECT
  _source_year,
  COUNT(*) AS rows,
  COUNT(DISTINCT _source_file) AS source_files
FROM company_ro.bronze.mfinante_uu_raw
GROUP BY _source_year
ORDER BY _source_year
"""))

# COMMAND ----------

# DBTITLE 1,Summary of bronze ingestion
display(spark.sql("""
SELECT
  MIN(_source_year) AS min_year,
  MAX(_source_year) AS max_year,
  COUNT(DISTINCT _source_year) AS number_of_years,
  COUNT(*) AS total_rows
FROM company_ro.bronze.mfinante_uu_raw
"""))
