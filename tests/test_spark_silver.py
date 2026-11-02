"""Bronze -> silver preparation and the metadata helpers, on local Spark."""
from datetime import datetime

import pytest

from ledgerlake import metadata, silver
from ledgerlake.registry import PARAMETERS, REGISTRY
from pipeline_helpers import FILES, T1, T2, bronze_df, build_silver, cfg_for, create_pipeline_runs


@pytest.fixture(scope="module")
def silver_tables(spark, generated):
    out, counts = generated
    return out, counts, build_silver(spark, out)


def test_silver_has_one_row_per_key(spark, silver_tables):
    for name in FILES:
        cfg = cfg_for(name)
        t = spark.table(f"silver.{name}")
        assert t.count() == t.select(*cfg.primary_keys).distinct().count(), name
        assert "_run_id" not in t.columns and "_silver_updated_at" in t.columns


def test_merge_keeps_latest_credit_pull(spark, silver_tables, generated):
    import csv

    out, _ = generated
    with open(out / "landing/credit_bureau_reports/credit_bureau_reports_batch_02.csv", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    latest = {}
    for r in rows:
        if r["customer_id"] not in latest or r["pulled_at"] > latest[r["customer_id"]]["pulled_at"]:
            latest[r["customer_id"]] = r
    sample = next(iter(latest.values()))
    got = spark.table("silver.credit_bureau_reports").where(f"customer_id = '{sample['customer_id']}'").first()
    assert got.report_id == sample["report_id"]
    assert spark.table("silver.credit_bureau_reports").count() == 150  # one per customer


def test_append_ignores_repeated_events(spark, silver_tables, generated):
    _, counts = generated
    events = spark.table("silver.payment_gateway_events")
    assert events.count() < counts["gateway_events_batch_01"] + counts["gateway_events_batch_02"]
    assert events.where("latency_ms IS NULL").count() == 3  # the three 'timeout' rows


def test_incremental_slice_and_watermark(spark, generated):
    out, counts = generated
    df, wm = silver.prepare(bronze_df(spark, out, "transactions"), cfg_for("transactions"), str(T1))
    assert df.count() == counts["transactions_batch_02"]
    assert wm.startswith("2026-10-08")
    empty, wm2 = silver.prepare(bronze_df(spark, out, "transactions"), cfg_for("transactions"), str(T2))
    assert empty.count() == 0 and wm2 is None


def test_full_load_takes_latest_snapshot(spark, generated):
    out, _ = generated
    b = bronze_df(spark, out, "branches")
    doubled = b.unionByName(b.withColumn("_ingested_at", b["_ingested_at"] + __import__("pyspark").sql.functions.expr("INTERVAL 1 DAY")))
    df, _ = silver.prepare(doubled, cfg_for("branches"), None)
    assert df.count() == b.count()


def test_metadata_reads_and_audit(spark):
    from pyspark.sql import functions as F

    spark.sql("CREATE DATABASE IF NOT EXISTS metadata")
    spark.createDataFrame(
        [(tid, n, s, o, l, order, True, d) for tid, n, s, o, l, order, d in REGISTRY],
        "table_id INT, table_name STRING, source_system STRING, source_object STRING, target_layer STRING, "
        "load_order INT, is_active BOOLEAN, description STRING",
    ).write.mode("overwrite").format("parquet").saveAsTable("metadata.table_registry")
    spark.createDataFrame([(tid, k, v) for tid, kv in PARAMETERS.items() for k, v in kv.items()],
                          "table_id INT, parameter_name STRING, parameter_value STRING") \
        .write.mode("overwrite").format("parquet").saveAsTable("metadata.table_parameters")
    spark.createDataFrame([(2, "2026-09-30 00:00:00", None, None, "r0")],
                          "table_id INT, source_watermark STRING, bronze_watermark STRING, "
                          "updated_at TIMESTAMP, last_run_id STRING") \
        .write.mode("overwrite").format("parquet").saveAsTable("metadata.table_watermarks")
    spark.sql("CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (run_id STRING, table_id INT, table_name STRING, "
              "layer STRING, start_time TIMESTAMP, end_time TIMESTAMP, status STRING, rows_processed BIGINT, "
              "error_message STRING) USING parquet")

    sql_tables = metadata.list_tables(spark, "spark_catalog", "sqlserver")
    assert [t["table_name"] for t in sql_tables] == ["branches", "customers", "accounts", "transactions"]
    gold_tables = metadata.list_tables(spark, "spark_catalog", "silver")
    assert len(gold_tables) == 6
    assert metadata.read_parameters(spark, "spark_catalog", 4)["load_type"] == "APPEND"
    assert metadata.get_watermarks(spark, "spark_catalog", 2)["source_watermark"] == "2026-09-30 00:00:00"
    assert metadata.get_watermarks(spark, "spark_catalog", 99) == {"source_watermark": None, "bronze_watermark": None}

    cfg = cfg_for("customers")
    metadata.record_run(spark, "spark_catalog", "r1", cfg, "bronze", metadata.utc_now(), "SUCCESS", 42)
    metadata.record_run(spark, "spark_catalog", "r1", cfg, "silver", metadata.utc_now(), "FAILED", 0, "x" * 5000)
    runs = spark.table("metadata.pipeline_runs").where(F.col("run_id") == "r1").orderBy("layer").collect()
    assert [(r.layer, r.status, r.rows_processed) for r in runs] == [("bronze", "SUCCESS", 42), ("silver", "FAILED", 0)]
    assert len(runs[1].error_message) == 4000


def test_audit_timestamps_are_utc(spark):
    """record_run must store the UTC instant whatever the Spark session time zone is."""
    create_pipeline_runs(spark)
    start = datetime(2026, 10, 23, 1, 2, 3)
    spark.conf.set("spark.sql.session.timeZone", "Australia/Sydney")
    try:
        metadata.record_run(spark, "spark_catalog", "tz", 1, "gold", start, "SUCCESS", 1)
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    got = spark.sql("SELECT CAST(start_time AS STRING) s FROM metadata.pipeline_runs WHERE run_id = 'tz'").first().s
    assert got == "2026-10-23 01:02:03"

