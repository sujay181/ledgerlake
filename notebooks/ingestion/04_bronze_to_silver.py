# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → silver (one table)
# MAGIC 1. Take the bronze rows not yet in silver (`_ingested_at` > `bronze_watermark`;
# MAGIC    FULL tables take the latest snapshot).
# MAGIC 2. Keep the newest row per primary key (by the table's watermark column).
# MAGIC 3. Write by load type: FULL overwrite, APPEND insert-new-keys-only, MERGE upsert.
# MAGIC 4. Move the bronze watermark forward and log the run.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))
from ledgerlake import config, metadata, silver

dbutils.widgets.text("catalog", config.DEFAULT_CATALOG)
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("table_metadata", '{"table_id": 2, "table_name": "customers", "source_system": "sqlserver", "source_object": "core.customers", "target_layer": "silver"}')
dbutils.widgets.text("table_parameters", '{"load_type": "MERGE", "primary_key": "customer_id", "watermark_column": "updated_at"}')

catalog = dbutils.widgets.get("catalog").strip()
run_id = dbutils.widgets.get("run_id").strip()
cfg = metadata.build_table_config(metadata.parse_param(dbutils.widgets.get("table_metadata")),
                                  metadata.parse_param(dbutils.widgets.get("table_parameters")))
bronze_table = config.fqn(catalog, config.BRONZE, cfg.table_name)
print(cfg)

# COMMAND ----------

start = metadata.utc_now()
rows = 0
try:
    if not spark.catalog.tableExists(bronze_table):
        print(f"{bronze_table} doesn't exist yet (no data has arrived): nothing to do")
    else:
        watermarks = metadata.get_watermarks(spark, catalog, cfg.table_id)
        print("Last bronze watermark:", watermarks["bronze_watermark"])
        prepared, new_watermark = silver.prepare(spark.table(bronze_table), cfg, watermarks["bronze_watermark"])
        rows = silver.write(spark, prepared, cfg, catalog)
        if new_watermark and cfg.load_type != "FULL":
            metadata.set_watermark(spark, catalog, cfg.table_id, "bronze_watermark", new_watermark, run_id)
            print("New bronze watermark:", new_watermark)
    metadata.record_run(spark, catalog, run_id, cfg, "silver", start, "SUCCESS", rows)
    print(f"{cfg.table_name}: {rows:,} rows written to silver ({cfg.load_type})")
except Exception as e:
    metadata.record_run(spark, catalog, run_id, cfg, "silver", start, "FAILED", rows, str(e))
    raise
