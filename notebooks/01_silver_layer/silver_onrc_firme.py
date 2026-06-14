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


def clean_upper(col):
    return F.upper(F.trim(col.cast("string")))


def clean_digits(col):
    return F.regexp_replace(col.cast("string"), r"[^0-9]", "")

# COMMAND ----------

# DBTITLE 1,Load and clean companies data
# Load bronze table
firme_raw = normalize_columns(spark.table("company_ro.bronze.onrc_firme_raw"))

# Clean and standardize companies
firme_cleaned = (
    firme_raw
    .select(
        clean_text(first_existing(firme_raw, ["COD_INMATRICULARE", "NR_INMATRICULARE", "NUMAR_INMATRICULARE"])).alias("cod_inmatriculare"),
        clean_digits(first_existing(firme_raw, ["CUI", "COD_FISCAL", "CIF", "COD_UNIC_INREGISTRARE"])).alias("cui"),
        clean_text(first_existing(firme_raw, ["DENUMIRE", "DENUMIRE_FIRMA", "NUME_FIRMA"])).alias("denumire"),
        clean_text(first_existing(firme_raw, ["FORMA_JURIDICA", "FORMA_ORGANIZARE", "TIP_FIRMA"])).alias("forma_juridica"),
        clean_digits(first_existing(firme_raw, ["COD_STARE_FIRMA", "COD_STARE", "STARE"])).alias("cod_stare_firma"),
        clean_upper(first_existing(firme_raw, ["JUDET", "DEN_JUDET", "DENUMIRE_JUDET", "ADR_JUDET"])).alias("judet"),
        clean_upper(first_existing(firme_raw, ["LOCALITATE", "DEN_LOCALITATE", "DENUMIRE_LOCALITATE", "ORAS", "MUNICIPIU"])).alias("localitate"),
        clean_text(first_existing(firme_raw, ["ADRESA", "ADRESA_COMPLETA", "ADR_COMPLETA"])).alias("adresa"),
        first_existing(firme_raw, ["_ingested_at", "INGESTED_AT"]).alias("_ingested_at"),
        first_existing(firme_raw, ["_source_file", "SOURCE_FILE"]).alias("_source_file")
    )
    .filter(F.col("cod_inmatriculare").isNotNull())
    .filter(F.col("cod_inmatriculare") != "")
    .dropDuplicates(["cod_inmatriculare"])
)

print(f"Cleaned {firme_cleaned.count():,} companies")

# COMMAND ----------

# DBTITLE 1,MERGE cleaned data into silver table (incremental)
from delta.tables import DeltaTable

# Create table if doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.silver.onrc_firme (
  cod_inmatriculare STRING,
  cui STRING,
  denumire STRING,
  forma_juridica STRING,
  cod_stare_firma STRING,
  judet STRING,
  localitate STRING,
  adresa STRING,
  _ingested_at TIMESTAMP,
  _source_file STRING
)
USING DELTA
""")

# MERGE new/updated companies (upsert)
delta_table = DeltaTable.forName(spark, "company_ro.silver.onrc_firme")

(
    delta_table.alias("target")
    .merge(
        firme_cleaned.alias("source"),
        "target.cod_inmatriculare = source.cod_inmatriculare"
    )
    .whenMatchedUpdateAll()  # Update existing records
    .whenNotMatchedInsertAll()  # Insert new records
    .execute()
)

print(f"✓ MERGED {firme_cleaned.count():,} companies into company_ro.silver.onrc_firme")
print(f"  Total rows in table: {spark.table('company_ro.silver.onrc_firme').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate data quality
# Data quality checks
silver_table = spark.table("company_ro.silver.onrc_firme")

print("\n=== Data Quality Summary ===")
print(f"Total rows: {silver_table.count():,}")
print(f"Unique CUI: {silver_table.select('cui').distinct().count():,}")
print(f"Null CUI: {silver_table.filter(F.col('cui').isNull() | (F.col('cui') == '')).count():,}")
print(f"Null denumire: {silver_table.filter(F.col('denumire').isNull() | (F.col('denumire') == '')).count():,}")
print(f"With forma_juridica: {silver_table.filter(F.col('forma_juridica').isNotNull() & (F.col('forma_juridica') != '')).count():,}")
print(f"With address: {silver_table.filter(F.col('adresa').isNotNull() & (F.col('adresa') != '')).count():,}")

# Legal form distribution
print("\n=== Top Legal Forms ===")
display(
    silver_table
    .groupBy("forma_juridica")
    .count()
    .orderBy(F.desc("count"))
    .limit(10)
)

# Sample records
print("\n=== Sample Records ===")
display(silver_table.limit(10))

# COMMAND ----------


