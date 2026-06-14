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
    """Normalize column names to ASCII uppercase with underscores"""
    value = value or ""
    value = unicodedata.normalize("NFKD", value)
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.upper().strip()
    value = re.sub(r"[^A-Z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")


def normalize_columns(df):
    """Normalize all column names in a DataFrame"""
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
    """Return first existing column from a list of candidates"""
    normalized_candidates = [normalize_name(c) for c in candidates]
    for candidate in normalized_candidates:
        if candidate in df.columns:
            return F.col(candidate)
    return F.lit(None)


def clean_text(col):
    """Clean and trim text column"""
    return F.trim(col.cast("string"))


def clean_digits(col):
    """Extract only digits from column"""
    return F.regexp_replace(col.cast("string"), r"[^0-9]", "")

# COMMAND ----------

# DBTITLE 1,Load and clean n_caen nomenclature
# Load bronze table
n_caen_raw = normalize_columns(spark.table("company_ro.bronze.n_caen_raw"))

# Clean and standardize CAEN nomenclature
n_caen_cleaned = (
    n_caen_raw
    .select(
        clean_digits(first_existing(n_caen_raw, ["COD", "COD_CAEN", "CAEN"])).alias("cod_caen"),
        clean_text(first_existing(n_caen_raw, ["DENUMIRE", "DENUMIRE_CAEN", "DEN_CAEN"])).alias("denumire_caen"),
        clean_text(first_existing(n_caen_raw, ["GRUPA", "GRUPA_CAEN", "SECTIUNEA"])).alias("grupa_caen"),
        clean_text(first_existing(n_caen_raw, ["VERSIUNE", "VER_CAEN", "VERSIUNE_CAEN"])).alias("versiune_caen"),
        first_existing(n_caen_raw, ["_ingested_at", "INGESTED_AT"]).alias("_ingested_at"),
        first_existing(n_caen_raw, ["_source_file", "SOURCE_FILE"]).alias("_source_file")
    )
    .filter(F.col("cod_caen").isNotNull())
    .filter(F.col("cod_caen") != "")
    .dropDuplicates(["cod_caen", "versiune_caen"])
)

print(f"Cleaned {n_caen_cleaned.count():,} CAEN codes")

# COMMAND ----------

# DBTITLE 1,MERGE cleaned data into silver table (incremental)
from delta.tables import DeltaTable

# Create table if doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.silver.n_caen (
  cod_caen STRING,
  denumire_caen STRING,
  grupa_caen STRING,
  versiune_caen STRING,
  _ingested_at TIMESTAMP,
  _source_file STRING
)
USING DELTA
""")

# MERGE new/updated records
delta_table = DeltaTable.forName(spark, "company_ro.silver.n_caen")

(
    delta_table.alias("target")
    .merge(
        n_caen_cleaned.alias("source"),
        "target.cod_caen = source.cod_caen AND target.versiune_caen = source.versiune_caen"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✓ MERGED {n_caen_cleaned.count():,} CAEN codes into company_ro.silver.n_caen")
print(f"  Total rows in table: {spark.table('company_ro.silver.n_caen').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate data quality
# Data quality checks
silver_table = spark.table("company_ro.silver.n_caen")

print("\n=== Data Quality Summary ===")
print(f"Total rows: {silver_table.count():,}")
print(f"Unique CAEN codes: {silver_table.select('cod_caen').distinct().count():,}")
print(f"Null cod_caen: {silver_table.filter(F.col('cod_caen').isNull()).count():,}")
print(f"Null denumire_caen: {silver_table.filter(F.col('denumire_caen').isNull()).count():,}")

# Sample records
print("\n=== Sample Records ===")
display(silver_table.orderBy("cod_caen").limit(10))

# COMMAND ----------


