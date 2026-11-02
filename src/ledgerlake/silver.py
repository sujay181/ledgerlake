"""Bronze -> silver. Silver holds one clean, current row per primary key.

Incremental slices use the bronze watermark (_ingested_at), not the business timestamp.
That way a file that arrives late with old business timestamps is still picked up.

Load types (from metadata.table_parameters):
  FULL    replace silver with the latest full snapshot in bronze
  APPEND  insert rows whose key is not in silver yet (re-runs never duplicate)
  MERGE   update rows whose key exists (only if the incoming row is newer), insert the rest
"""
from __future__ import annotations

from . import config
from .metadata import TableConfig

BRONZE_ONLY_COLUMNS = ["_run_id", "_batch_id"]


def select_slice(bronze_df, cfg: TableConfig, last_bronze_watermark: str | None):
    """Rows of bronze that silver hasn't processed yet."""
    from pyspark.sql import functions as F

    if cfg.load_type == "FULL":
        latest = bronze_df.agg(F.max("_ingested_at").alias("m")).first()["m"]
        return bronze_df.where(F.col("_ingested_at") == F.lit(latest)) if latest else bronze_df.limit(0)
    if last_bronze_watermark:
        return bronze_df.where(F.col("_ingested_at") > F.lit(last_bronze_watermark).cast("timestamp"))
    return bronze_df


def latest_per_key(df, keys: list[str], order_columns: list[str]):
    """Keep the newest row per key. Two versions of a key in one batch would make MERGE fail."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_columns])
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def prepare(bronze_df, cfg: TableConfig, last_bronze_watermark: str | None):
    """Return (rows to write to silver, new bronze watermark or None if nothing new)."""
    from pyspark.sql import functions as F

    sliced = select_slice(bronze_df, cfg, last_bronze_watermark)
    new_watermark = sliced.agg(F.max("_ingested_at").cast("string").alias("m")).first()["m"]
    order = ([cfg.watermark_column] if cfg.watermark_column else []) + ["_ingested_at"]
    clean = (latest_per_key(sliced, list(cfg.primary_keys), order)
             .drop(*[c for c in BRONZE_ONLY_COLUMNS if c in sliced.columns])
             .withColumn("_silver_updated_at", F.current_timestamp()))
    return clean, new_watermark


def merge_condition(keys: tuple[str, ...] | list[str]) -> str:
    return " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)


def write(spark, df, cfg: TableConfig, catalog: str) -> int:
    """Write the prepared rows to silver according to the table's load type."""
    from delta.tables import DeltaTable

    target = config.fqn(catalog, config.SILVER, cfg.table_name)
    rows = df.count()
    if rows == 0:
        return 0
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        return rows
    if cfg.load_type == "FULL":
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        return rows

    builder = DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), merge_condition(cfg.primary_keys))
    if hasattr(builder, "withSchemaEvolution"):  # new source columns flow through to silver
        builder = builder.withSchemaEvolution()
    if cfg.load_type == "MERGE":
        wm = cfg.watermark_column
        builder = builder.whenMatchedUpdateAll(condition=f"s.`{wm}` >= t.`{wm}`")
    builder.whenNotMatchedInsertAll().execute()
    return rows
