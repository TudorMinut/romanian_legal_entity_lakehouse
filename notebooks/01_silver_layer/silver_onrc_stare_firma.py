# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Initialize catalog and helper functions
from pyspark.sql import functions as F
import re
import unicodedata

spark.sql("CREATE CATALOG IF NOT EXISTS company_ro")
spark.sql("CREATE SCHEMA IF NOT EXISTS company_ro.silver")


def normalize_name(value: str) -> str:
    value = value or ""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.upper().strip()
    value = re.sub(r"[^A-Z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def normalize_columns(df):
    used = {}
    for old_name in df.columns:
        new_name = normalize_name(old_name)
        if new_name in used:
            used[new_name] += 1
            new_name = f"{new_name}_{used[new_name]}"
        else:
            used[new_name] = 1
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


def first_existing(df, candidates):
    normalized_candidates = [normalize_name(c) for c in candidates]
    for candidate in normalized_candidates:
        if candidate in df.columns:
            return F.col(candidate)
    return F.lit(None)


def clean_text(col):
    return F.trim(col.cast("string"))


def clean_digits(col):
    return F.regexp_replace(col.cast("string"), r"[^0-9]", "")

# COMMAND ----------

# DBTITLE 1,Load and clean company status data
# Load bronze table
stare_raw = normalize_columns(spark.table("company_ro.bronze.onrc_stare_firma_raw"))

# Clean and standardize company status (one status per company)
stare_cleaned = (
    stare_raw
    .select(
        clean_text(first_existing(stare_raw, ["COD_INMATRICULARE", "NR_INMATRICULARE", "NUMAR_INMATRICULARE"])).alias("cod_inmatriculare"),
        clean_digits(first_existing(stare_raw, ["COD", "COD_STARE", "COD_STARE_FIRMA"])).alias("cod_stare_firma"),
        first_existing(stare_raw, ["_ingested_at", "INGESTED_AT"]).alias("_ingested_at"),
        first_existing(stare_raw, ["_source_file", "SOURCE_FILE"]).alias("_source_file")
    )
    .filter(F.col("cod_inmatriculare").isNotNull())
    .filter(F.col("cod_inmatriculare") != "")
    .dropDuplicates(["cod_inmatriculare"])
)

print(f"Cleaned {stare_cleaned.count():,} company status records")

# COMMAND ----------

# DBTITLE 1,MERGE cleaned data into silver table (incremental)
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.silver.onrc_stare_firma (
  cod_inmatriculare STRING, cod_stare_firma STRING, _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.silver.onrc_stare_firma")
(delta_table.alias("target").merge(stare_cleaned.alias("source"), "target.cod_inmatriculare = source.cod_inmatriculare").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED {stare_cleaned.count():,} company status records")
print(f"  Total: {spark.table('company_ro.silver.onrc_stare_firma').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate data quality
# Data quality checks
silver_table = spark.table("company_ro.silver.onrc_stare_firma")

print("\n=== Data Quality Summary ===")
print(f"Total rows: {silver_table.count():,}")
print(f"Unique companies: {silver_table.select('cod_inmatriculare').distinct().count():,}")
print(f"Unique status codes: {silver_table.select('cod_stare_firma').distinct().count():,}")
print(f"Null cod_inmatriculare: {silver_table.filter(F.col('cod_inmatriculare').isNull()).count():,}")
print(f"Null cod_stare_firma: {silver_table.filter(F.col('cod_stare_firma').isNull()).count():,}")

# Status code distribution
print("\n=== Status Code Distribution ===")
display(
    silver_table
    .groupBy("cod_stare_firma")
    .count()
    .orderBy(F.desc("count"))
)

# Sample records
print("\n=== Sample Records ===")
display(silver_table.limit(10))
