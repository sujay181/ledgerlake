# Databricks notebook source
# MAGIC %md
# MAGIC # Email the run summary
# MAGIC Reads this run's rows from `metadata.pipeline_runs` and emails an HTML summary
# MAGIC through Gmail SMTP. The master job runs this task even when earlier tasks fail.
# MAGIC Without `recipients` or the Gmail secret it just prints the summary.

# COMMAND ----------

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "..", "src")))
from ledgerlake import config, notify

dbutils.widgets.text("catalog", config.DEFAULT_CATALOG)
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("sender", "")
dbutils.widgets.text("recipients", "")
dbutils.widgets.text("bank_name", "Coral Digital Bank")

catalog = dbutils.widgets.get("catalog").strip()
run_id = dbutils.widgets.get("run_id").strip()
sender = dbutils.widgets.get("sender").strip()
recipients = [r.strip() for r in dbutils.widgets.get("recipients").split(",") if r.strip()]
bank_name = dbutils.widgets.get("bank_name").strip()

# COMMAND ----------

m = f"{catalog}.{config.METADATA}"
runs_df = spark.sql(
    f"SELECT {', '.join(notify.COLUMNS)} FROM {m}.pipeline_runs WHERE run_id = :run_id ORDER BY start_time",
    args={"run_id": run_id})
rows = notify.latest_attempts([r.asDict() for r in runs_df.collect()])

# Every silver table logs a bronze and a silver row; every gold table logs one gold row
expected = spark.sql(f"""
    SELECT COALESCE(SUM(CASE WHEN target_layer = 'silver' THEN 2 ELSE 1 END), 0) AS n
    FROM {m}.table_registry WHERE is_active""").first()["n"]
status = notify.overall_status(rows, expected)
print(f"Run {run_id}: {status} ({len(rows)} of {expected} table loads logged)")
display(runs_df)

# COMMAND ----------

html_body = notify.build_html(rows, run_id, status, bank_name)
if not (sender and recipients):
    print("No sender/recipients set: email skipped")
else:
    try:
        app_password = dbutils.secrets.get(config.SECRET_SCOPE, config.SMTP_SECRET_KEY)
    except Exception:
        app_password = None
        print(f"Secret {config.SMTP_SECRET_KEY} not found: email skipped")
    if app_password:
        notify.send_gmail(sender, app_password, recipients, notify.subject(status, bank_name), html_body)
        print(f"Summary emailed to {', '.join(recipients)}")
