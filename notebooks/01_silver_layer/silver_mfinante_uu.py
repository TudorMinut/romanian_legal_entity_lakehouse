# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Initialize catalog and schema
from pyspark.sql import functions as F

spark.sql("CREATE CATALOG IF NOT EXISTS company_ro")
spark.sql("CREATE SCHEMA IF NOT EXISTS company_ro.silver")

def first_existing(df, candidates):
    """Return first existing column from a list of candidates"""
    for candidate in candidates:
        if candidate in df.columns:
            return F.col(candidate)
    return F.lit(None)

# COMMAND ----------

# DBTITLE 1,Load bronze data and normalize column names
# Load bronze table
mfin_raw = spark.table("company_ro.bronze.mfinante_uu_raw")

# Identify positional columns (_c0, _c1, etc.)
raw_data_cols = [
    c for c in mfin_raw.columns
    if c.startswith("_c") and c[2:].isdigit()
]

# Sort by numeric position
raw_data_cols = sorted(
    raw_data_cols,
    key=lambda c: int(c.replace("_c", ""))
)

# Rename positional columns to mfin_col_XX
named_fin = mfin_raw
for i, old_col in enumerate(raw_data_cols):
    named_fin = named_fin.withColumnRenamed(old_col, f"mfin_col_{i:02d}")

print(f"Renamed {len(raw_data_cols)} positional columns")
print(f"Total rows: {named_fin.count():,}")

# COMMAND ----------

# DBTITLE 1,Define data cleaning functions
def clean_digits(col):
    """Extract only digits from column"""
    return F.regexp_replace(col.cast("string"), r"[^0-9]", "")


def to_decimal(col):
    """Convert to decimal, handling European number format"""
    cleaned = F.regexp_replace(F.trim(col.cast("string")), r"[^0-9,\.\-]", "")
    cleaned = F.regexp_replace(cleaned, ",", ".")
    return cleaned.cast("decimal(20,2)")


def to_int(col):
    """Convert to integer"""
    return clean_digits(col).cast("int")


def clean_text(col):
    """Clean and trim text"""
    return F.trim(col.cast("string"))

# COMMAND ----------

# DBTITLE 1,Transform and clean financial data
# Transform and clean the data
mfinante_cleaned = (
    named_fin
    # Remove embedded header rows from each TXT file
    .filter(F.upper(F.col("mfin_col_00")) != "CUI")
    .select(
        clean_digits(F.col("mfin_col_00")).alias("cui"),
        F.col("_source_year").cast("int").alias("an"),
        clean_digits(F.col("mfin_col_01")).alias("cod_caen_mfinante"),
        to_decimal(F.col("mfin_col_02")).alias("active_imobilizate"),
        to_decimal(F.col("mfin_col_03")).alias("active_circulante"),
        to_decimal(F.col("mfin_col_04")).alias("stocuri"),
        to_decimal(F.col("mfin_col_05")).alias("creante"),
        to_decimal(F.col("mfin_col_06")).alias("casa_si_conturi"),
        to_decimal(F.col("mfin_col_07")).alias("cheltuieli_in_avans"),
        to_decimal(F.col("mfin_col_08")).alias("datorii"),
        to_decimal(F.col("mfin_col_09")).alias("venituri_in_avans"),
        to_decimal(F.col("mfin_col_10")).alias("provizioane"),
        to_decimal(F.col("mfin_col_11")).alias("capitaluri_proprii"),
        to_decimal(F.col("mfin_col_12")).alias("capital_social"),
        to_decimal(F.col("mfin_col_13")).alias("cifra_afaceri"),
        to_decimal(F.col("mfin_col_14")).alias("venituri_totale"),
        to_decimal(F.col("mfin_col_15")).alias("cheltuieli_totale"),
        to_decimal(F.col("mfin_col_16")).alias("profit_brut"),
        to_decimal(F.col("mfin_col_17")).alias("pierdere_bruta"),
        to_decimal(F.col("mfin_col_18")).alias("profit_net"),
        to_decimal(F.col("mfin_col_19")).alias("pierdere_neta"),
        to_int(F.col("mfin_col_20")).alias("nr_mediu_salariati"),
        first_existing(named_fin, ["_ingested_at", "INGESTED_AT"]).alias("_ingested_at"),
        first_existing(named_fin, ["_source_file", "SOURCE_FILE"]).alias("_source_file")
    )
    .filter(F.col("cui").isNotNull())
    .filter(F.col("cui") != "")
    .filter(F.col("an").isNotNull())
    .dropDuplicates(["cui", "an"])
)

print(f"Cleaned {mfinante_cleaned.count():,} financial records")

# COMMAND ----------

# DBTITLE 1,MERGE cleaned data into silver table (incremental)
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.silver.mfinante_uu (
  cui STRING, an INT, cod_caen_mfinante STRING, active_imobilizate DECIMAL(20,2), active_circulante DECIMAL(20,2), 
  stocuri DECIMAL(20,2), creante DECIMAL(20,2), casa_si_conturi DECIMAL(20,2), cheltuieli_in_avans DECIMAL(20,2), 
  datorii DECIMAL(20,2), venituri_in_avans DECIMAL(20,2), provizioane DECIMAL(20,2), capitaluri_proprii DECIMAL(20,2), 
  capital_social DECIMAL(20,2), cifra_afaceri DECIMAL(20,2), venituri_totale DECIMAL(20,2), cheltuieli_totale DECIMAL(20,2), 
  profit_brut DECIMAL(20,2), pierdere_bruta DECIMAL(20,2), profit_net DECIMAL(20,2), pierdere_neta DECIMAL(20,2), 
  nr_mediu_salariati INT, _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.silver.mfinante_uu")
(delta_table.alias("target").merge(mfinante_cleaned.alias("source"), "target.cui = source.cui AND target.an = source.an").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED {mfinante_cleaned.count():,} financial records")
print(f"  Total: {spark.table('company_ro.silver.mfinante_uu').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate data quality
# Data quality checks
silver_table = spark.table("company_ro.silver.mfinante_uu")

print("\n=== Data Quality Summary ===")
print(f"Total rows: {silver_table.count():,}")
print(f"Unique companies: {silver_table.select('cui').distinct().count():,}")
print(f"Years covered: {silver_table.select('an').distinct().count():,}")
print(f"Null CUI: {silver_table.filter(F.col('cui').isNull()).count():,}")
print(f"Null an: {silver_table.filter(F.col('an').isNull()).count():,}")

# Data by year
print("\n=== Financial Data by Year ===")
display(
    silver_table.groupBy("an").agg(
        F.count("*").alias("rows"),
        F.count("cifra_afaceri").alias("with_cifra_afaceri"),
        F.count("profit_net").alias("with_profit_net"),
        F.count("nr_mediu_salariati").alias("with_salariati")
    ).orderBy("an")
)

# Sample records
print("\n=== Sample Records ===")
display(silver_table.orderBy(F.desc("an"), F.desc("cifra_afaceri")).limit(10))

# COMMAND ----------


