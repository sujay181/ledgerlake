# Databricks notebook source
# MAGIC %md
# MAGIC # Build one gold table
# MAGIC Runs `gold_sql/<table_name>.sql` as `CREATE OR REPLACE TABLE gold.<table_name> AS ...`
# MAGIC and logs the result. Adding a gold table = add a SQL file + a registry row.

# COMMAND ----------

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))
from ledgerlake import config, gold, metadata

dbutils.widgets.text("catalog", config.DEFAULT_CATALOG)
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("table_metadata", '{"table_id": 11, "table_name": "customer_360", "source_system": "silver", "target_layer": "gold"}')

catalog = dbutils.widgets.get("catalog").strip()
run_id = dbutils.widgets.get("run_id").strip()
cfg = metadata.build_table_config(metadata.parse_param(dbutils.widgets.get("table_metadata")))
if cfg.target_layer != "gold":
    raise ValueError(f"{cfg.table_name} is not a gold table")

# COMMAND ----------

start = metadata.utc_now()
try:
    rows = gold.build_table(spark, catalog, cfg.table_name, os.path.join(REPO_ROOT, "gold_sql"))
    metadata.record_run(spark, catalog, run_id, cfg, "gold", start, "SUCCESS", rows)
    print(f"gold.{cfg.table_name}: {rows:,} rows")
except Exception as e:
    metadata.record_run(spark, catalog, run_id, cfg, "gold", start, "FAILED", None, str(e))
    raise
