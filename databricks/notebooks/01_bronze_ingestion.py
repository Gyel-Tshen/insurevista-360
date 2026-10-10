# Databricks notebook source
# MAGIC %md
# MAGIC Config

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "dbw_insurevista_dev"
SCHEMA  = "hollard_bronze"
BRONZE  = "abfss://bronze@hollardlake.dfs.core.windows.net"

# ADF will pass its run ID here later; "manual" when run by hand
dbutils.widgets.text("batch_id", "manual")
batch_id = dbutils.widgets.get("batch_id")
print("batch_id =", batch_id)

# COMMAND ----------

# MAGIC %md
# MAGIC The 10 sources

# COMMAND ----------

# (table_name, file_format, source_folder, source_system)
SOURCES = [
    ("policies",                "parquet", f"{BRONZE}/sqlserver/policies/",                 "SQL_Server"),
    ("claims",                  "parquet", f"{BRONZE}/sqlserver/claims/",                   "SQL_Server"),
    ("customers",               "parquet", f"{BRONZE}/sqlserver/customers/",                "SQL_Server"),
    ("brokers",                 "parquet", f"{BRONZE}/sqlserver/brokers/",                  "SQL_Server"),
    ("premium_transactions",    "parquet", f"{BRONZE}/sqlserver/premium_transactions/",     "SQL_Server"),
    ("motor_vehicle_register",  "csv",     f"{BRONZE}/csv/motor_vehicle_register/",         "Broker_Feed"),
    ("property_risk_details",   "csv",     f"{BRONZE}/csv/property_risk_details/",          "Broker_Feed"),
    ("customer_complaints_log", "csv",     f"{BRONZE}/csv/customer_complaints_log/",        "CRM"),
    ("partner_brand_sales",     "csv",     f"{BRONZE}/csv/partner_brand_sales/",            "Partner_Portal"),
    ("catastrophe_risk_scores", "parquet", f"{BRONZE}/json_flattened/catastrophe_risk_scores/", "RiskFrontiers_API"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC The ingestion function

# COMMAND ----------

def ingest(table, fmt, source_path, source_system):
    target     = f"{CATALOG}.{SCHEMA}.{table}"
    delta_path = f"{BRONZE}/delta/{table}/"
    checkpoint = f"{BRONZE}/_checkpoints/{table}/"

    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0

    reader = (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", fmt)
                .option("cloudFiles.schemaLocation", f"{checkpoint}schema/"))
    if fmt == "csv":
        reader = reader.option("header", "true")   # CSV columns stay STRING in Bronze; Silver casts

    df = (reader.load(source_path)
            .withColumn("_ingestion_ts",  F.current_timestamp())
            .withColumn("_source_system", F.lit(source_system))
            .withColumn("_source_file",   F.col("_metadata.file_path"))
            .withColumn("_batch_id",      F.lit(batch_id)))

    query = (df.writeStream
               .format("delta")
               .option("checkpointLocation", checkpoint)
               .trigger(availableNow=True)
               .start(delta_path))
    query.awaitTermination()

    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} USING DELTA LOCATION '{delta_path}'")

    after = spark.table(target).count()
    print(f"{table:<26} +{after - before:>8,} rows   (total {after:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC Run all sources

# COMMAND ----------

for table, fmt, path, system in SOURCES:
    ingest(table, fmt, path, system)