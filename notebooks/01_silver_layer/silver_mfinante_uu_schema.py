# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Initialize catalog and schema
from pyspark.sql import functions as F

spark.sql("CREATE CATALOG IF NOT EXISTS company_ro")
spark.sql("CREATE SCHEMA IF NOT EXISTS company_ro.silver")

# COMMAND ----------

# DBTITLE 1,Load bronze schema metadata
# Load bronze schema metadata table
schema_raw = spark.table("company_ro.bronze.mfinante_uu_schema_raw")

print(f"Total rows: {schema_raw.count():,}")
display(schema_raw.groupBy("_source_year").count().orderBy("_source_year"))

# COMMAND ----------

# DBTITLE 1,Clean and validate schema metadata
# Clean schema metadata
schema_cleaned = (
    schema_raw
    .select(
        F.trim(F.col("value")).alias("schema_definition"),
        F.col("_source_year").cast("int").alias("source_year"),
        F.col("_ingested_at"),
        F.col("_source_file")
    )
    .filter(F.col("schema_definition").isNotNull())
    .filter(F.col("schema_definition") != "")
    .dropDuplicates(["source_year", "schema_definition"])
)

print(f"Cleaned {schema_cleaned.count():,} schema metadata records")

# COMMAND ----------

# DBTITLE 1,Write to silver table
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.silver.mfinante_uu_schema (
  schema_definition STRING, source_year INT, _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.silver.mfinante_uu_schema")
(delta_table.alias("target").merge(schema_cleaned.alias("source"), "target.source_year = source.source_year AND target.schema_definition = source.schema_definition").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED {schema_cleaned.count():,} schema metadata records")
print(f"  Total: {spark.table('company_ro.silver.mfinante_uu_schema').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate data quality
# Data quality checks
silver_table = spark.table("company_ro.silver.mfinante_uu_schema")

print("\n=== Data Quality Summary ===")
print(f"Total rows: {silver_table.count():,}")
print(f"Years covered: {silver_table.select('source_year').distinct().count():,}")
print(f"Null schema_definition: {silver_table.filter(F.col('schema_definition').isNull()).count():,}")

# Schema by year
print("\n=== Schema Metadata by Year ===")
display(
    silver_table.groupBy("source_year").agg(
        F.count("*").alias("rows")
    ).orderBy("source_year")
)

# Sample records
print("\n=== Sample Records ===")
display(silver_table.orderBy("source_year").limit(20))

# COMMAND ----------


