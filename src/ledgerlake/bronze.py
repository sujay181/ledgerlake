"""Source -> bronze. Bronze keeps every row exactly as it arrived, plus load columns.

Load columns added to every bronze row:
  _ingested_at  when the row landed in bronze (drives the bronze -> silver watermark)
  _run_id       the job run that loaded it
  _batch_id     unique id of the write, so this run's rows can be counted precisely
  _source_file  (files only) the file the row came from
"""
from __future__ import annotations

import uuid

from . import config
from .metadata import TableConfig, check_identifier, check_qualified_name, check_watermark_value


def jdbc_query(source_object: str, watermark_column: str | None, last_watermark: str | None) -> str:
    """Subquery pushed down to SQL Server. Incremental when a watermark exists."""
    check_qualified_name(source_object)
    if watermark_column and last_watermark:
        check_identifier(watermark_column)
        check_watermark_value(last_watermark)
        return (f"(SELECT * FROM {source_object} "
                f"WHERE {watermark_column} > CAST('{last_watermark}' AS DATETIME2)) AS src")
    return f"(SELECT * FROM {source_object}) AS src"


def jdbc_url(conn: dict) -> str:
    return (f"jdbc:sqlserver://{conn['host']}:{conn.get('port', 1433)};database={conn['database']};"
            "encrypt=true;trustServerCertificate=false;loginTimeout=60")


def _with_load_columns(df, run_id: str, batch_id: str):
    from pyspark.sql import functions as F

    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_run_id", F.lit(str(run_id)))
              .withColumn("_batch_id", F.lit(batch_id)))


def sqlserver_to_bronze(spark, cfg: TableConfig, catalog: str, run_id: str, conn: dict,
                        last_source_watermark: str | None) -> tuple[int, str | None]:
    """Read new rows from SQL Server and append them to bronze.

    Returns (rows loaded, new source watermark). FULL tables are re-read completely."""
    from pyspark.sql import functions as F

    use_watermark = cfg.is_incremental
    query = jdbc_query(cfg.source_object, cfg.watermark_column if use_watermark else None,
                       last_source_watermark if use_watermark else None)
    df = (spark.read.format("jdbc")
          .option("url", jdbc_url(conn))
          .option("dbtable", query)
          .option("user", conn["user"])
          .option("password", conn["password"])
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
          .option("fetchsize", 10000)
          .load())
    batch_id = str(uuid.uuid4())
    target = config.fqn(catalog, config.BRONZE, cfg.table_name)
    (_with_load_columns(df, run_id, batch_id)
        .write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target))

    # Count from what was written instead of re-reading the source
    written = spark.table(target).where(F.col("_batch_id") == batch_id)
    stats = written.agg(
        F.count(F.lit(1)).alias("n"),
        (F.max(cfg.watermark_column).cast("string") if use_watermark else F.lit(None).cast("string")).alias("wm"),
    ).first()
    return int(stats["n"]), stats["wm"]


def files_to_bronze(spark, cfg: TableConfig, catalog: str, run_id: str) -> int:
    """Pick up new files from the landing volume with Auto Loader and append them to bronze.

    The checkpoint remembers which files were already loaded, so each file loads once.
    availableNow processes everything new and then stops, which suits a scheduled job."""
    from pyspark.sql import functions as F

    source_path = f"{config.landing_root(catalog)}/{cfg.source_object}"
    batch_id = str(uuid.uuid4())
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", cfg.file_format)
              .option("cloudFiles.schemaLocation", config.schema_path(catalog, cfg.table_name))
              .option("cloudFiles.inferColumnTypes", "true")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
              .option("header", "true"))
    if cfg.schema_hints:
        # Pin the types that matter. A value that doesn't fit (e.g. "timeout" in an INT column)
        # becomes NULL and the original text is kept in the _rescued_data column.
        reader = reader.option("cloudFiles.schemaHints", cfg.schema_hints)
    stream = reader.load(source_path).withColumn("_source_file", F.col("_metadata.file_path"))
    query = (_with_load_columns(stream, run_id, batch_id)
             .writeStream.format("delta")
             .option("checkpointLocation", config.checkpoint_path(catalog, cfg.table_name))
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable(config.fqn(catalog, config.BRONZE, cfg.table_name)))
    query.awaitTermination()
    return rows_from_progress(query.recentProgress)


def rows_from_progress(progress) -> int:
    """Sum numInputRows over a stream's progress reports (dicts on Spark 3.5, objects on newer runtimes)."""
    total = 0
    for p in progress or []:
        value = p.get("numInputRows", 0) if isinstance(p, dict) else getattr(p, "numInputRows", 0)
        total += int(value or 0)
    return total
