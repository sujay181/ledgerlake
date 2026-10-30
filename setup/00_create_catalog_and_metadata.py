# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Create the catalog, schemas, landing volume and metadata tables
# MAGIC
# MAGIC Run once (safe to re-run). It creates:
# MAGIC * catalog `ledgerlake` with schemas `metadata`, `source`, `bronze`, `silver`, `gold`
# MAGIC * volume `source.landing` where the CSV files for Auto Loader are uploaded
# MAGIC * the four metadata tables, seeded from `src/ledgerlake/registry.py`
# MAGIC
# MAGIC If your workspace won't let you create a catalog, set the `catalog` widget to `workspace`
# MAGIC and use the same value as the `catalog` job parameter.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from ledgerlake import config
from ledgerlake.registry import PARAMETERS, REGISTRY

dbutils.widgets.text("catalog", config.DEFAULT_CATALOG)
dbutils.widgets.dropdown("reset_watermarks", "false", ["false", "true"])
catalog = dbutils.widgets.get("catalog").strip()
reset_watermarks = dbutils.widgets.get("reset_watermarks") == "true"
print(f"Catalog: {catalog} | reset watermarks: {reset_watermarks}")

# COMMAND ----------

# DBTITLE 1,Catalog, schemas, volume
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
except Exception as e:
    raise RuntimeError(
        f"Could not create catalog '{catalog}'. Re-run with catalog = workspace "
        f"(and pass catalog=workspace to the jobs). Original error: {e}"
    )

for schema in config.ALL_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{config.SOURCE}.{config.LANDING_VOLUME}")
for row in REGISTRY:
    if row[2] == "files":
        dbutils.fs.mkdirs(f"{config.landing_root(catalog)}/{row[3]}")
print("Landing folders:", [f.name for f in dbutils.fs.ls(config.landing_root(catalog))])

# COMMAND ----------

# DBTITLE 1,Metadata tables
m = f"{catalog}.{config.METADATA}"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {m}.table_registry (
    table_id       INT     COMMENT 'Unique id of the logical table',
    table_name     STRING  COMMENT 'Name used in bronze, silver and gold',
    source_system  STRING  COMMENT 'sqlserver | files | silver (gold tables)',
    source_object  STRING  COMMENT 'schema.table for sqlserver, landing folder for files, NULL for gold',
    target_layer   STRING  COMMENT 'silver or gold',
    load_order     INT     COMMENT 'Processing order within a source system',
    is_active      BOOLEAN COMMENT 'Inactive tables are skipped',
    description    STRING,
    created_at     TIMESTAMP
) COMMENT 'Which tables the pipeline loads'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {m}.table_parameters (
    table_id         INT,
    parameter_name   STRING COMMENT 'load_type | primary_key | watermark_column | file_format | schema_hints',
    parameter_value  STRING,
    created_at       TIMESTAMP
) COMMENT 'How each table loads'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {m}.table_watermarks (
    table_id          INT,
    source_watermark  STRING COMMENT 'Max business watermark read from the source (SQL Server extraction)',
    bronze_watermark  STRING COMMENT 'Max bronze _ingested_at already merged into silver',
    updated_at        TIMESTAMP,
    last_run_id       STRING
) COMMENT 'How far each table has been loaded'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {m}.pipeline_runs (
    run_id          STRING,
    table_id        INT,
    table_name      STRING,
    layer           STRING COMMENT 'bronze | silver | gold',
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    status          STRING COMMENT 'SUCCESS | FAILED',
    rows_processed  BIGINT,
    error_message   STRING
) COMMENT 'Append-only audit log: one row per table, per layer, per run'""")

# COMMAND ----------

# DBTITLE 1,Seed the registry and parameters (replaces their contents)
from pyspark.sql import functions as F

registry_df = spark.createDataFrame(
    [(tid, name, src, obj, layer, order, True, desc) for tid, name, src, obj, layer, order, desc in REGISTRY],
    "table_id INT, table_name STRING, source_system STRING, source_object STRING, target_layer STRING, "
    "load_order INT, is_active BOOLEAN, description STRING",
).withColumn("created_at", F.current_timestamp())
registry_df.write.mode("overwrite").saveAsTable(f"{m}.table_registry")

params_df = spark.createDataFrame(
    [(tid, k, v) for tid, kv in PARAMETERS.items() for k, v in kv.items()],
    "table_id INT, parameter_name STRING, parameter_value STRING",
).withColumn("created_at", F.current_timestamp())
params_df.write.mode("overwrite").saveAsTable(f"{m}.table_parameters")

if reset_watermarks:
    spark.sql(f"DELETE FROM {m}.table_watermarks")
    print("Watermarks cleared: the next run reloads everything from the start")

display(spark.table(f"{m}.table_registry").orderBy("source_system", "load_order"))
display(spark.table(f"{m}.table_parameters").orderBy("table_id", "parameter_name"))
