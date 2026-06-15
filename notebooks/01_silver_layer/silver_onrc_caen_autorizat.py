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

# DBTITLE 1,Load and clean company CAEN authorizations
# Load bronze table
caen_raw = normalize_columns(spark.table("company_ro.bronze.onrc_caen_autorizat_raw"))

# Clean and standardize company-CAEN relationships
caen_cleaned = (
    caen_raw
    .select(
        clean_text(first_existing(caen_raw, ["COD_INMATRICULARE", "NR_INMATRICULARE", "NUMAR_INMATRICULARE"])).alias("cod_inmatriculare"),
        clean_digits(first_existing(caen_raw, ["COD_CAEN_AUTORIZAT", "COD_CAEN", "CAEN"])).alias("cod_caen"),
        clean_text(first_existing(caen_raw, ["VER_CAEN_AUTORIZAT", "VERSIUNE_CAEN", "VER_CAEN"])).alias("versiune_caen"),
        first_existing(caen_raw, ["_ingested_at", "INGESTED_AT"]).alias("_ingested_at"),
        first_existing(caen_raw, ["_source_file", "SOURCE_FILE"]).alias("_source_file")
    )
    .filter(F.col("cod_inmatriculare").isNotNull())
    .filter(F.col("cod_inmatriculare") != "")
    .filter(F.col("cod_caen").isNotNull())
    .filter(F.col("cod_caen") != "")
    .dropDuplicates(["cod_inmatriculare", "cod_caen", "versiune_caen"])
)

print(f"Cleaned {caen_cleaned.count():,} company-CAEN relationships")

# COMMAND ----------

# DBTITLE 1,MERGE cleaned data into silver table (incremental)
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.silver.onrc_caen_autorizat (
  cod_inmatriculare STRING, cod_caen STRING, versiune_caen STRING, _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.silver.onrc_caen_autorizat")
(delta_table.alias("target").merge(caen_cleaned.alias("source"), "target.cod_inmatriculare = source.cod_inmatriculare AND target.cod_caen = source.cod_caen AND target.versiune_caen = source.versiune_caen").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED {caen_cleaned.count():,} CAEN authorizations")
print(f"  Total: {spark.table('company_ro.silver.onrc_caen_autorizat').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate data quality
# Data quality checks
silver_table = spark.table("company_ro.silver.onrc_caen_autorizat")

print("\n=== Data Quality Summary ===")
print(f"Total rows: {silver_table.count():,}")
print(f"Unique companies: {silver_table.select('cod_inmatriculare').distinct().count():,}")
print(f"Unique CAEN codes: {silver_table.select('cod_caen').distinct().count():,}")
print(f"Null cod_inmatriculare: {silver_table.filter(F.col('cod_inmatriculare').isNull()).count():,}")
print(f"Null cod_caen: {silver_table.filter(F.col('cod_caen').isNull()).count():,}")

# Companies with multiple CAEN codes
print("\n=== Companies with Multiple CAEN Codes ===")
multi_caen = (
    silver_table
    .groupBy("cod_inmatriculare")
    .agg(F.count("*").alias("caen_count"))
    .filter(F.col("caen_count") > 1)
)
print(f"Companies with multiple CAEN codes: {multi_caen.count():,}")

# Sample records
print("\n=== Sample Records ===")
display(silver_table.limit(20))
