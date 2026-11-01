# Databricks notebook source
# MAGIC %md
# MAGIC # Read the load parameters for one table
# MAGIC Turns the key/value rows in `metadata.table_parameters` into one object
# MAGIC (`load_type`, `primary_key`, `watermark_column`, ...) and validates it,
# MAGIC so a bad config fails here, before any data moves.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))
from ledgerlake import config, metadata

EXAMPLE = '{"table_id": 2, "table_name": "customers", "source_system": "sqlserver", "source_object": "core.customers", "target_layer": "silver"}'
dbutils.widgets.text("catalog", config.DEFAULT_CATALOG)
dbutils.widgets.text("table_metadata", EXAMPLE)
catalog = dbutils.widgets.get("catalog").strip()
table_metadata = metadata.parse_param(dbutils.widgets.get("table_metadata"))

# COMMAND ----------

params = metadata.read_parameters(spark, catalog, int(table_metadata["table_id"]))
metadata.build_table_config(table_metadata, params)  # raises if the config is incomplete
print(json.dumps(params, indent=2))

dbutils.jobs.taskValues.set(key="table_parameters", value=params)
