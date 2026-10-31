# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Store connection secrets
# MAGIC
# MAGIC Puts two secrets in the `ledgerlake` secret scope, using the Databricks SDK
# MAGIC (it authenticates as you, so no token is copied or printed):
# MAGIC * `sqlserver-connection`: JSON with host, port, database, user, password
# MAGIC * `gmail-app-password`: a Gmail **app password** for the run-summary email
# MAGIC
# MAGIC **After running:** clear the widgets (top bar > ⋮ > Remove all widgets) and clear the
# MAGIC notebook outputs, so no password stays in the notebook. The Databricks CLI alternative
# MAGIC is in the README.

# COMMAND ----------

import json
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from ledgerlake import config

dbutils.widgets.text("sql_host", "your-server.database.windows.net")
dbutils.widgets.text("sql_port", "1433")
dbutils.widgets.text("sql_database", "coralbank")
dbutils.widgets.text("sql_user", "")
dbutils.widgets.text("sql_password", "")
dbutils.widgets.text("gmail_app_password", "")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
if config.SECRET_SCOPE not in [s.name for s in w.secrets.list_scopes()]:
    w.secrets.create_scope(scope=config.SECRET_SCOPE)
    print(f"Created secret scope {config.SECRET_SCOPE}")

if dbutils.widgets.get("sql_password"):
    connection = {
        "host": dbutils.widgets.get("sql_host").strip(),
        "port": int(dbutils.widgets.get("sql_port")),
        "database": dbutils.widgets.get("sql_database").strip(),
        "user": dbutils.widgets.get("sql_user").strip(),
        "password": dbutils.widgets.get("sql_password"),
    }
    w.secrets.put_secret(scope=config.SECRET_SCOPE, key=config.SQLSERVER_SECRET_KEY,
                         string_value=json.dumps(connection))
    print(f"Saved {config.SQLSERVER_SECRET_KEY}")

if dbutils.widgets.get("gmail_app_password"):
    w.secrets.put_secret(scope=config.SECRET_SCOPE, key=config.SMTP_SECRET_KEY,
                         string_value=dbutils.widgets.get("gmail_app_password").replace(" ", ""))
    print(f"Saved {config.SMTP_SECRET_KEY}")

print("Keys in scope:", [s.key for s in w.secrets.list_secrets(scope=config.SECRET_SCOPE)])

# COMMAND ----------

# DBTITLE 1,Test the SQL Server connection
conn = json.loads(dbutils.secrets.get(config.SECRET_SCOPE, config.SQLSERVER_SECRET_KEY))
from ledgerlake.bronze import jdbc_url

test = (spark.read.format("jdbc")
        .option("url", jdbc_url(conn))
        .option("dbtable", "(SELECT COUNT(*) AS customers FROM core.customers) AS t")
        .option("user", conn["user"]).option("password", conn["password"])
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load())
display(test)
