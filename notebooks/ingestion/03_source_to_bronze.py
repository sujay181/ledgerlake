# Databricks notebook source
# MAGIC %md
# MAGIC # Source → bronze (one table)
# MAGIC * **sqlserver**: JDBC read of rows newer than the table's `source_watermark`
# MAGIC   (FULL tables are read completely), appended to bronze. The source watermark moves
# MAGIC   forward only after the bronze write succeeds.
# MAGIC * **files**: Auto Loader picks up new files in the landing volume (`availableNow`),
# MAGIC   appended to bronze. The checkpoint makes sure each file loads once.
# MAGIC
# MAGIC Every attempt writes a row to `metadata.pipeline_runs`, success or failure.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))
from ledgerlake import bronze, config, metadata

dbutils.widgets.text("catalog", config.DEFAULT_CATALOG)
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("table_metadata", '{"table_id": 2, "table_name": "customers", "source_system": "sqlserver", "source_object": "core.customers", "target_layer": "silver"}')
dbutils.widgets.text("table_parameters", '{"load_type": "MERGE", "primary_key": "customer_id", "watermark_column": "updated_at"}')

catalog = dbutils.widgets.get("catalog").strip()
run_id = dbutils.widgets.get("run_id").strip()
cfg = metadata.build_table_config(metadata.parse_param(dbutils.widgets.get("table_metadata")),
                                  metadata.parse_param(dbutils.widgets.get("table_parameters")))
print(cfg)

# COMMAND ----------

start = metadata.utc_now()
rows = 0
try:
    if cfg.source_system == "sqlserver":
        conn = json.loads(dbutils.secrets.get(config.SECRET_SCOPE, config.SQLSERVER_SECRET_KEY))
        watermarks = metadata.get_watermarks(spark, catalog, cfg.table_id)
        print("Last source watermark:", watermarks["source_watermark"])
        rows, new_watermark = bronze.sqlserver_to_bronze(
            spark, cfg, catalog, run_id, conn, watermarks["source_watermark"])
        if cfg.is_incremental and new_watermark:
            metadata.set_watermark(spark, catalog, cfg.table_id, "source_watermark", new_watermark, run_id)
            print("New source watermark:", new_watermark)
    elif cfg.source_system == "files":
        rows = bronze.files_to_bronze(spark, cfg, catalog, run_id)
    else:
        raise ValueError(f"{cfg.table_name}: source_system {cfg.source_system} has no bronze step")

    metadata.record_run(spark, catalog, run_id, cfg, "bronze", start, "SUCCESS", rows)
    print(f"{cfg.table_name}: {rows:,} rows appended to bronze")
except Exception as e:
    metadata.record_run(spark, catalog, run_id, cfg, "bronze", start, "FAILED", rows, str(e))
    raise

dbutils.jobs.taskValues.set(key="bronze_rows", value=rows)
