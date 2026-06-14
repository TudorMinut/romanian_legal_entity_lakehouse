# Databricks notebook source
# DBTITLE 1,Initialize catalog and helper functions
from pyspark.sql import functions as F

spark.sql("CREATE CATALOG IF NOT EXISTS company_ro")
spark.sql("CREATE SCHEMA IF NOT EXISTS company_ro.gold")


def hash_key(*cols):
    """Generate hash key from multiple columns"""
    return F.sha2(
        F.concat_ws("||", *[F.coalesce(c.cast("string"), F.lit("")) for c in cols]),
        256
    )

# COMMAND ----------

# DBTITLE 1,Load silver tables
# Load all clean silver tables
firme = spark.table("company_ro.silver.onrc_firme")
caen_auth = spark.table("company_ro.silver.onrc_caen_autorizat")
stare = spark.table("company_ro.silver.onrc_stare_firma")
n_caen = spark.table("company_ro.silver.n_caen")
n_stare = spark.table("company_ro.silver.n_stare_firma")
mfinante = spark.table("company_ro.silver.mfinante_uu")

print("Loaded silver tables:")
print(f"  Companies: {firme.count():,}")
print(f"  CAEN authorizations: {caen_auth.count():,}")
print(f"  Company statuses: {stare.count():,}")
print(f"  CAEN nomenclature: {n_caen.count():,}")
print(f"  Status nomenclature: {n_stare.count():,}")
print(f"  Financial data: {mfinante.count():,}")

# COMMAND ----------

# DBTITLE 1,Create dim_stare_firma (status dimension)
from delta.tables import DeltaTable

# Dimension: Company Status
dim_stare_firma = n_stare

# Create table if doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.dim_stare_firma (
  cod_stare_firma STRING,
  stare_firma STRING,
  is_activa BOOLEAN,
  _ingested_at TIMESTAMP,
  _source_file STRING
)
USING DELTA
""")

# MERGE (upsert) dimension
delta_table = DeltaTable.forName(spark, "company_ro.gold.dim_stare_firma")

(
    delta_table.alias("target")
    .merge(
        dim_stare_firma.alias("source"),
        "target.cod_stare_firma = source.cod_stare_firma"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print(f"✓ MERGED company_ro.gold.dim_stare_firma: {dim_stare_firma.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create dim_caen (CAEN dimension)
# Dimension: CAEN Codes
dim_caen = n_caen

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.dim_caen (
  cod_caen STRING, denumire_caen STRING, grupa_caen STRING, versiune_caen STRING, _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.gold.dim_caen")
(delta_table.alias("target").merge(dim_caen.alias("source"), "target.cod_caen = source.cod_caen AND target.versiune_caen = source.versiune_caen").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED company_ro.gold.dim_caen: {dim_caen.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create dim_forma_juridica (legal form dimension)
# Dimension: Legal Form
dim_forma_juridica = (
    firme
    .select("forma_juridica")
    .filter(F.col("forma_juridica").isNotNull())
    .filter(F.col("forma_juridica") != "")
    .dropDuplicates(["forma_juridica"])
    .withColumn("forma_juridica_key", hash_key(F.col("forma_juridica")))
    .select("forma_juridica_key", "forma_juridica")
)

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.dim_forma_juridica (
  forma_juridica_key STRING, forma_juridica STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.gold.dim_forma_juridica")
(delta_table.alias("target").merge(dim_forma_juridica.alias("source"), "target.forma_juridica_key = source.forma_juridica_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

print(f"✓ Created company_ro.gold.dim_forma_juridica: {dim_forma_juridica.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create dim_localitate (location dimension)
# Dimension: Location (county + city)
dim_localitate = (
    firme
    .select("judet", "localitate")
    .filter(F.col("judet").isNotNull() | F.col("localitate").isNotNull())
    .dropDuplicates(["judet", "localitate"])
    .withColumn("localitate_key", hash_key(F.col("judet"), F.col("localitate")))
    .select("localitate_key", "judet", "localitate")
)

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.dim_localitate (
  localitate_key STRING, judet STRING, localitate STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.gold.dim_localitate")
(delta_table.alias("target").merge(dim_localitate.alias("source"), "target.localitate_key = source.localitate_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

print(f"✓ Created company_ro.gold.dim_localitate: {dim_localitate.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create dim_adresa (address dimension)
# Dimension: Address
dim_adresa = (
    firme
    .select("judet", "localitate", "adresa")
    .filter(F.col("adresa").isNotNull())
    .filter(F.col("adresa") != "")
    .dropDuplicates(["judet", "localitate", "adresa"])
    .withColumn("localitate_key", hash_key(F.col("judet"), F.col("localitate")))
    .withColumn("adresa_key", hash_key(F.col("localitate_key"), F.col("adresa")))
    .select("adresa_key", "localitate_key", "adresa")
)

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.dim_adresa (
  adresa_key STRING, localitate_key STRING, adresa STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.gold.dim_adresa")
(delta_table.alias("target").merge(dim_adresa.alias("source"), "target.adresa_key = source.adresa_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

print(f"✓ Created company_ro.gold.dim_adresa: {dim_adresa.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create dim_firma (company dimension)
# Dimension: Company (main dimension)
dim_firma = (
    firme
    .withColumn("firma_key", hash_key(F.col("cod_inmatriculare"), F.col("cui")))
    .withColumn(
        "forma_juridica_key",
        F.when(
            F.col("forma_juridica").isNotNull() & (F.col("forma_juridica") != ""),
            hash_key(F.col("forma_juridica"))
        )
    )
    .withColumn(
        "adresa_localitate_key",
        F.when(
            F.col("judet").isNotNull() | F.col("localitate").isNotNull(),
            hash_key(F.col("judet"), F.col("localitate"))
        )
    )
    .withColumn(
        "adresa_key",
        F.when(
            F.col("adresa").isNotNull() & (F.col("adresa") != ""),
            hash_key(F.col("adresa_localitate_key"), F.col("adresa"))
        )
    )
    .select(
        "firma_key",
        "cod_inmatriculare",
        "cui",
        "denumire",
        "cod_stare_firma",
        "forma_juridica_key",
        "adresa_key",
        "_ingested_at",
        "_source_file"
    )
)

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.dim_firma (
  firma_key STRING, cod_inmatriculare STRING, cui STRING, denumire STRING, cod_stare_firma STRING, 
  forma_juridica_key STRING, adresa_key STRING, _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.gold.dim_firma")
(delta_table.alias("target").merge(dim_firma.alias("source"), "target.firma_key = source.firma_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED company_ro.gold.dim_firma: {dim_firma.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create bridge_firma_caen (many-to-many bridge)
# Bridge Table: Company to CAEN (many-to-many)
bridge_firma_caen = (
    caen_auth
    .withColumn(
        "firma_caen_key",
        hash_key(F.col("cod_inmatriculare"), F.col("cod_caen"), F.col("versiune_caen"))
    )
    .select(
        "firma_caen_key",
        "cod_inmatriculare",
        "cod_caen",
        "versiune_caen",
        "_ingested_at",
        "_source_file"
    )
)

spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.bridge_firma_caen (
  firma_caen_key STRING, cod_inmatriculare STRING, cod_caen STRING, versiune_caen STRING, 
  _ingested_at TIMESTAMP, _source_file STRING
) USING DELTA
""")

delta_table = DeltaTable.forName(spark, "company_ro.gold.bridge_firma_caen")
(delta_table.alias("target").merge(bridge_firma_caen.alias("source"), "target.firma_caen_key = source.firma_caen_key").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
print(f"✓ MERGED company_ro.gold.bridge_firma_caen: {bridge_firma_caen.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Create fact_financiar_anual (fact table)
# Fact Table: Annual Financial Data
fact_financiar_anual = (
    mfinante
    .withColumn(
        "financiar_key",
        hash_key(F.col("cui"), F.col("an"))
    )
    .select(
        "financiar_key",
        "cui",
        "an",
        "cod_caen_mfinante",
        "active_imobilizate",
        "active_circulante",
        "stocuri",
        "creante",
        "casa_si_conturi",
        "cheltuieli_in_avans",
        "datorii",
        "venituri_in_avans",
        "provizioane",
        "capitaluri_proprii",
        "capital_social",
        "cifra_afaceri",
        "venituri_totale",
        "cheltuieli_totale",
        "profit_brut",
        "pierdere_bruta",
        "profit_net",
        "pierdere_neta",
        "nr_mediu_salariati",
        "_ingested_at",
        "_source_file"
    )
)

# Create table if doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS company_ro.gold.fact_financiar_anual (
  financiar_key STRING,
  cui STRING,
  an INT,
  cod_caen_mfinante STRING,
  active_imobilizate DECIMAL(18,2),
  active_circulante DECIMAL(18,2),
  stocuri DECIMAL(18,2),
  creante DECIMAL(18,2),
  casa_si_conturi DECIMAL(18,2),
  cheltuieli_in_avans DECIMAL(18,2),
  datorii DECIMAL(18,2),
  venituri_in_avans DECIMAL(18,2),
  provizioane DECIMAL(18,2),
  capitaluri_proprii DECIMAL(18,2),
  capital_social DECIMAL(18,2),
  cifra_afaceri DECIMAL(18,2),
  venituri_totale DECIMAL(18,2),
  cheltuieli_totale DECIMAL(18,2),
  profit_brut DECIMAL(18,2),
  pierdere_bruta DECIMAL(18,2),
  profit_net DECIMAL(18,2),
  pierdere_neta DECIMAL(18,2),
  nr_mediu_salariati INT,
  _ingested_at TIMESTAMP,
  _source_file STRING
)
USING DELTA
""")

# Get existing years in fact table
existing_years = (
    spark.table("company_ro.gold.fact_financiar_anual")
    .select("an")
    .distinct()
    .rdd.flatMap(lambda x: x)
    .collect()
)

# Filter to only NEW years (facts are immutable)
new_facts = fact_financiar_anual.filter(~F.col("an").isin(existing_years))

if new_facts.count() == 0:
    print("✓ No new financial years to add. All years already in fact table.")
else:
    # APPEND new facts (never overwrite or update facts)
    (
        new_facts
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("company_ro.gold.fact_financiar_anual")
    )
    print(f"✓ APPENDED {new_facts.count():,} new facts to company_ro.gold.fact_financiar_anual")

print(f"  Total rows in table: {spark.table('company_ro.gold.fact_financiar_anual').count():,}")

# COMMAND ----------

# DBTITLE 1,Validate dimensional model
# Summary of created tables
print("\n=== Gold Dimensional Model Summary ===")

tables = [
    "dim_stare_firma",
    "dim_forma_juridica",
    "dim_localitate",
    "dim_adresa",
    "dim_caen",
    "dim_firma",
    "bridge_firma_caen",
    "fact_financiar_anual"
]

for table_name in tables:
    count = spark.table(f"company_ro.gold.{table_name}").count()
    print(f"  {table_name}: {count:,} rows")

print("\n✓ Gold dimensional model created successfully!")

# COMMAND ----------


