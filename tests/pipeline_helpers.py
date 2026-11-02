"""Shared helpers for the local Spark tests: simulate bronze from the generated CSVs,
prepare silver with the real framework code, and run gold SQL files.

Delta Lake isn't available locally, so tables are stored as Parquet. The Delta MERGE and
CREATE OR REPLACE steps are exercised on Databricks instead.
"""
from datetime import datetime
from pathlib import Path

from ledgerlake import metadata
from ledgerlake.registry import PARAMETERS, REGISTRY

ROOT = Path(__file__).resolve().parents[1]
T1 = datetime(2026, 10, 1, 0, 0, 0)
T2 = datetime(2026, 10, 8, 0, 0, 0)

SCHEMAS = {
    "branches": "branch_id STRING, bsb STRING, branch_name STRING, city STRING, state STRING, region STRING, "
                "opened_date DATE, updated_at TIMESTAMP",
    "customers": "customer_id STRING, first_name STRING, last_name STRING, date_of_birth DATE, email STRING, "
                 "mobile STRING, state STRING, postcode STRING, home_branch_id STRING, kyc_status STRING, "
                 "customer_since DATE, updated_at TIMESTAMP",
    "accounts": "account_id STRING, customer_id STRING, product_type STRING, bsb STRING, account_status STRING, "
                "opened_date DATE, interest_rate DECIMAL(6,4), credit_limit DECIMAL(12,2), "
                "current_balance DECIMAL(14,2), updated_at TIMESTAMP",
    "transactions": "transaction_id STRING, account_id STRING, txn_timestamp TIMESTAMP, direction STRING, "
                    "channel STRING, amount DECIMAL(14,2), merchant_category STRING, description STRING, "
                    "txn_status STRING",
    "credit_bureau_reports": "report_id STRING, customer_id STRING, bureau_name STRING, pulled_at TIMESTAMP, "
                             "credit_score INT, risk_band STRING, open_credit_accounts INT, defaults_last_5y INT, "
                             "external_debt_aud DECIMAL(14,2)",
    "payment_gateway_events": "event_id STRING, transaction_id STRING, gateway STRING, event_timestamp TIMESTAMP, "
                              "event_status STRING, response_code STRING, latency_ms INT, fee_aud DECIMAL(10,2)",
}
FILES = {
    "branches": ["sqlserver/batch_01_historical/branches.csv"],
    "customers": ["sqlserver/batch_01_historical/customers.csv", "sqlserver/batch_02_incremental/customers.csv"],
    "accounts": ["sqlserver/batch_01_historical/accounts.csv", "sqlserver/batch_02_incremental/accounts.csv"],
    "transactions": ["sqlserver/batch_01_historical/transactions.csv", "sqlserver/batch_02_incremental/transactions.csv"],
    "credit_bureau_reports": ["landing/credit_bureau_reports/credit_bureau_reports_batch_01.csv",
                              "landing/credit_bureau_reports/credit_bureau_reports_batch_02.csv"],
    "payment_gateway_events": ["landing/payment_gateway_events/payment_gateway_events_batch_01.csv",
                               "landing/payment_gateway_events/payment_gateway_events_batch_02.csv"],
}


def cfg_for(name):
    row = next(r for r in REGISTRY if r[1] == name)
    tid, _, source, obj, layer, _o, _d = row
    return metadata.build_table_config({"table_id": tid, "table_name": name, "source_system": source,
                                        "source_object": obj, "target_layer": layer}, PARAMETERS[tid])


def bronze_df(spark, out, name):
    """Simulate bronze: batch 01 landed at T1, batch 02 at T2, plus the load columns."""
    from pyspark.sql import functions as F

    frames = []
    for i, rel in enumerate(FILES[name]):
        df = (spark.read.option("header", "true").option("mode", "PERMISSIVE")
              .schema(SCHEMAS[name]).csv(str(out / rel))
              .withColumn("_ingested_at", F.lit(str([T1, T2][i])).cast("timestamp"))
              .withColumn("_run_id", F.lit(f"run{i + 1}"))
              .withColumn("_batch_id", F.lit(f"b{i + 1}")))
        frames.append(df)
    result = frames[0]
    for f in frames[1:]:
        result = result.unionByName(f)
    return result


def build_silver(spark, out):
    """Prepare every silver table from both generated batches and save it as silver.<name>."""
    from ledgerlake import silver

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    watermarks = {}
    for name in FILES:
        df, wm = silver.prepare(bronze_df(spark, out, name), cfg_for(name), None)
        df.write.mode("overwrite").format("parquet").saveAsTable(f"silver.{name}")
        watermarks[name] = wm
    return watermarks


def run_gold(spark, name):
    from ledgerlake import gold

    sql = gold.render(gold.load_sql(ROOT / "gold_sql", name), "spark_catalog")
    return spark.sql(sql)


GOLD_TABLES = ["customer_360", "branch_scorecard", "channel_daily_summary", "bank_daily_kpis",
               "credit_risk_watchlist", "payment_gateway_daily"]


def build_gold(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    for name in GOLD_TABLES:
        run_gold(spark, name).write.mode("overwrite").format("parquet").saveAsTable(f"gold.{name}")


def create_pipeline_runs(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS metadata")
    spark.sql("CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (run_id STRING, table_id INT, table_name STRING, "
              "layer STRING, start_time TIMESTAMP, end_time TIMESTAMP, status STRING, rows_processed BIGINT, "
              "error_message STRING) USING parquet")
