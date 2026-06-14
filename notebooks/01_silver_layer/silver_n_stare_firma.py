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

# DBTITLE 1,Load and clean company status nomenclature
# Load bronze table
n_stare_raw = normalize_columns(spark.table("company_ro.bronze.n_stare_firma_raw"))

# Clean and standardize company status nomenclature
n_stare_cleaned = (
    n_stare_raw
    .select(
        clean_digits(first_existing(n_stare_raw, ["COD", "COD_STARE", "COD_STARE_FIRMA"])).alias("cod_stare_firma"),
        clean_text(first_existing(n_stare_raw, ["DENUMIRE", "DENUMIRE_STARE", "STARE_FIRMA"])).alias("stare_firma"),
        first_existing(n_stare_raw, ["_ingested_at", "INGESTED_AT"]).alias("_ingested_at"),
        first_existing(n_stare_raw, ["_source_file", "SOURCE_FILE"]).alias("_source_file")
    )
    .filter(F.col("cod_stare_firma").isNotNull())
    .filter(F.col("cod_stare_firma") != "")
    .dropDuplicates(["cod_stare_firma"])
    .withColumn(
        "is_activa",
        F.when(F.lower(F.col("stare_firma")) == "functiune", F.lit(True))
         .when(F.lower(F.col("stare_firma")) == "funcțiune", F.lit(True))
         .otherwise(F.lit(False))
    )
)

print(f"Cleaned {n_stare_cleaned.count():,} company status codes")

# COMMAND ----------

# DBTITLE 1,MERGE cleaned data into silver table (incremental)
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.silver.n_stare_firma (
  cod_stare_firma STRING, stare_firma STRING, is_activa BOOLEAN, _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.silver.n_stare_firma")
(delta_table.alias("target").merge(n_stare_cleaned.alias("source"), "target.cod_stare_firma = source.cod_stare_firma").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED {n_stare_cleaned.count():,} status codes")
print(f"  Total: {spark.table('company_ro.silver.n_stare_firma').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate data quality
# Data quality checks
silver_table = spark.table("company_ro.silver.n_stare_firma")

print("\n=== Data Quality Summary ===")
print(f"Total rows: {silver_table.count():,}")
print(f"Active statuses: {silver_table.filter(F.col('is_activa') == True).count():,}")
print(f"Inactive statuses: {silver_table.filter(F.col('is_activa') == False).count():,}")
print(f"Null cod_stare_firma: {silver_table.filter(F.col('cod_stare_firma').isNull()).count():,}")

# Sample records
print("\n=== Sample Records ===")
display(silver_table.orderBy("cod_stare_firma"))

# COMMAND ----------


