# Databricks notebook source
# MAGIC %md
# MAGIC # List tables for a source system
# MAGIC Reads `metadata.table_registry` and hands the active tables to the next task as the
# MAGIC task value `tables`. The job's For-each task loops over that list.
# MAGIC
# MAGIC `source_system`: `sqlserver`, `files`, or `silver` (gold tables).

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))
from ledgerlake import config, metadata

dbutils.widgets.text("catalog", config.DEFAULT_CATALOG)
dbutils.widgets.text("source_system", "sqlserver")
catalog = dbutils.widgets.get("catalog").strip()
source_system = dbutils.widgets.get("source_system").strip()

# COMMAND ----------

tables = metadata.list_tables(spark, catalog, source_system)
if not tables:
    print(f"No active tables for source_system={source_system}")
print(json.dumps(tables, indent=2))

dbutils.jobs.taskValues.set(key="tables", value=tables)
