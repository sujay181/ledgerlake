# Databricks notebook source
# MAGIC %md
# MAGIC # Check the pipeline
# MAGIC Quick views of the metadata tables and row counts per layer. Handy after each run.

# COMMAND ----------

dbutils.widgets.text("catalog", "ledgerlake")
catalog = dbutils.widgets.get("catalog").strip()
m = f"{catalog}.metadata"

# COMMAND ----------

# DBTITLE 1,Latest runs
display(spark.sql(f"""
    SELECT run_id, layer, table_name, status, rows_processed, start_time, end_time,
           round(timestampdiff(SECOND, start_time, end_time), 0) AS seconds, error_message
    FROM {m}.pipeline_runs
    ORDER BY start_time DESC
    LIMIT 100"""))

# COMMAND ----------

# DBTITLE 1,Watermarks
display(spark.sql(f"""
    SELECT r.table_id, r.table_name, w.source_watermark, w.bronze_watermark, w.updated_at, w.last_run_id
    FROM {m}.table_registry r LEFT JOIN {m}.table_watermarks w USING (table_id)
    WHERE r.target_layer = 'silver' ORDER BY r.table_id"""))

# COMMAND ----------

# DBTITLE 1,Row counts by layer
counts = []
for r in spark.table(f"{m}.table_registry").collect():
    layers = ["bronze", "silver"] if r.target_layer == "silver" else ["gold"]
    for layer in layers:
        t = f"{catalog}.{layer}.{r.table_name}"
        n = spark.table(t).count() if spark.catalog.tableExists(t) else None
        counts.append((r.table_name, layer, n))
display(spark.createDataFrame(counts, "table_name STRING, layer STRING, rows BIGINT"))

# COMMAND ----------

# DBTITLE 1,Rows Auto Loader rescued (values that didn't fit the expected type)
display(spark.sql(f"""
    SELECT 'payment_gateway_events' AS table_name, _source_file, _rescued_data
    FROM {catalog}.bronze.payment_gateway_events WHERE _rescued_data IS NOT NULL
    UNION ALL
    SELECT 'credit_bureau_reports', _source_file, _rescued_data
    FROM {catalog}.bronze.credit_bureau_reports WHERE _rescued_data IS NOT NULL"""))
