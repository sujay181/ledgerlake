"""Read and write the metadata tables that drive the pipeline.

metadata.table_registry    which tables exist and where they come from
metadata.table_parameters  how each table loads (load type, keys, watermark column)
metadata.table_watermarks  how far each table has been loaded
metadata.pipeline_runs     one audit row per table, per layer, per run (append-only)
"""
from __future__ import annotations

import ast
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from . import config

VALID_SOURCE_SYSTEMS = {"sqlserver", "files", "silver"}
VALID_LOAD_TYPES = {"FULL", "APPEND", "MERGE"}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_WATERMARK_VALUE = re.compile(r"^[0-9T:\-. ]{4,32}$")


# --------------------------------------------------------------------------- pure helpers

def utc_now() -> datetime:
    """Naive UTC timestamp (Spark stores TIMESTAMP in the session time zone, UTC on Databricks)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_param(value: Any) -> Any:
    """Parse a widget / task-value string into a Python object.

    Task values arrive as JSON. Values typed by hand in a widget are often Python-style
    dicts with single quotes, so fall back to ast.literal_eval (never eval)."""
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        raise ValueError("Empty parameter")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return ast.literal_eval(text)


def check_identifier(name: str) -> str:
    """Allow only plain SQL identifiers, because table and column names are put into SQL text."""
    if not name or not _IDENTIFIER.match(name):
        raise ValueError(f"Unsafe or empty identifier: {name!r}")
    return name


def check_qualified_name(name: str) -> str:
    parts = name.split(".")
    if not 1 <= len(parts) <= 3:
        raise ValueError(f"Bad qualified name: {name!r}")
    for p in parts:
        check_identifier(p)
    return name


def check_watermark_value(value: str) -> str:
    if not _WATERMARK_VALUE.match(value):
        raise ValueError(f"Unexpected watermark value: {value!r}")
    return value


def split_keys(primary_key: str | None) -> list[str]:
    """'a, b' -> ['a', 'b'] (composite keys are comma separated)."""
    if not primary_key:
        return []
    return [check_identifier(k.strip()) for k in primary_key.split(",") if k.strip()]


@dataclass(frozen=True)
class TableConfig:
    table_id: int
    table_name: str
    source_system: str
    source_object: str | None
    target_layer: str
    load_type: str | None = None
    primary_keys: tuple[str, ...] = ()
    watermark_column: str | None = None
    file_format: str = "csv"
    schema_hints: str | None = None

    @property
    def is_incremental(self) -> bool:
        return self.load_type in ("APPEND", "MERGE") and bool(self.watermark_column)


def build_table_config(table_metadata: dict, table_parameters: dict | None = None) -> TableConfig:
    """Validate one registry row plus its parameters and return a typed config."""
    p = table_parameters or {}
    source_system = str(table_metadata["source_system"]).lower()
    if source_system not in VALID_SOURCE_SYSTEMS:
        raise ValueError(f"Unsupported source_system {source_system!r}")
    target_layer = str(table_metadata.get("target_layer") or "silver").lower()
    load_type = p.get("load_type")
    if target_layer == "silver":
        if load_type not in VALID_LOAD_TYPES:
            raise ValueError(f"Table {table_metadata['table_name']}: load_type must be one of {VALID_LOAD_TYPES}")
        if not split_keys(p.get("primary_key")):
            raise ValueError(f"Table {table_metadata['table_name']}: primary_key is required")
        if load_type in ("APPEND", "MERGE") and not p.get("watermark_column"):
            raise ValueError(f"Table {table_metadata['table_name']}: {load_type} needs a watermark_column")
    source_object = table_metadata.get("source_object") or None
    if source_object and source_system == "sqlserver":
        check_qualified_name(source_object)
    wm = p.get("watermark_column")
    return TableConfig(
        table_id=int(table_metadata["table_id"]),
        table_name=check_identifier(str(table_metadata["table_name"])),
        source_system=source_system,
        source_object=source_object,
        target_layer=target_layer,
        load_type=load_type,
        primary_keys=tuple(split_keys(p.get("primary_key"))),
        watermark_column=check_identifier(wm) if wm else None,
        file_format=p.get("file_format", "csv"),
        schema_hints=p.get("schema_hints") or None,
    )


def registry_row_to_dict(row) -> dict:
    """Spark Row -> JSON-safe dict (task values must be JSON serialisable)."""
    d = row.asDict() if hasattr(row, "asDict") else dict(row)
    return {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in d.items()}


def retry_on_conflict(fn: Callable[[], Any], attempts: int = 6, base_delay: float = 2.0) -> Any:
    """Retry a Delta write when parallel tasks touch the same metadata table."""
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:  # Delta raises several Concurrent*Exception types
            is_conflict = "Concurrent" in type(e).__name__ or "CONCURRENT" in str(e).upper()
            if not is_conflict or i == attempts - 1:
                raise
            time.sleep(base_delay * (i + 1))


# --------------------------------------------------------------------------- Spark helpers

def list_tables(spark, catalog: str, source_system: str) -> list[dict]:
    """Active registry rows for one source system, in load order."""
    if source_system.lower() not in VALID_SOURCE_SYSTEMS:
        raise ValueError(f"Unsupported source_system {source_system!r}")
    df = spark.sql(
        f"""SELECT table_id, table_name, source_system, source_object, target_layer, load_order
            FROM {catalog}.{config.METADATA}.table_registry
            WHERE is_active AND lower(source_system) = :source_system
            ORDER BY load_order, table_id""",
        args={"source_system": source_system.lower()},
    )
    return [registry_row_to_dict(r) for r in df.collect()]


def read_parameters(spark, catalog: str, table_id: int) -> dict:
    rows = spark.sql(
        f"""SELECT parameter_name, parameter_value
            FROM {catalog}.{config.METADATA}.table_parameters
            WHERE table_id = :table_id""",
        args={"table_id": int(table_id)},
    ).collect()
    return {r.parameter_name: r.parameter_value for r in rows}


def get_watermarks(spark, catalog: str, table_id: int) -> dict:
    """{'source_watermark': str|None, 'bronze_watermark': str|None}"""
    rows = spark.sql(
        f"""SELECT source_watermark, bronze_watermark
            FROM {catalog}.{config.METADATA}.table_watermarks WHERE table_id = :table_id""",
        args={"table_id": int(table_id)},
    ).collect()
    if not rows:
        return {"source_watermark": None, "bronze_watermark": None}
    return {"source_watermark": rows[0].source_watermark, "bronze_watermark": rows[0].bronze_watermark}


def set_watermark(spark, catalog: str, table_id: int, column: str, value: str, run_id: str) -> None:
    """Upsert one watermark column (source_watermark or bronze_watermark) for a table."""
    if column not in ("source_watermark", "bronze_watermark"):
        raise ValueError(column)
    sql = f"""
        MERGE INTO {catalog}.{config.METADATA}.table_watermarks t
        USING (SELECT :table_id AS table_id, :value AS value, :run_id AS run_id) s
        ON t.table_id = s.table_id
        WHEN MATCHED THEN UPDATE SET {column} = s.value, updated_at = current_timestamp(), last_run_id = s.run_id
        WHEN NOT MATCHED THEN INSERT (table_id, {column}, updated_at, last_run_id)
             VALUES (s.table_id, s.value, current_timestamp(), s.run_id)"""
    retry_on_conflict(lambda: spark.sql(sql, args={"table_id": int(table_id), "value": str(value), "run_id": str(run_id)}))


AUDIT_COLUMNS = ["run_id", "table_id", "table_name", "layer", "start_time", "end_time", "status",
                 "rows_processed", "error_message"]


def record_run(spark, catalog: str, run_id: str, cfg_or_id, layer: str, start_time: datetime,
               status: str, rows_processed: int | None = None, error_message: str | None = None) -> None:
    """Append one audit row. Append-only, so parallel tasks never conflict on it."""
    table_id = cfg_or_id.table_id if isinstance(cfg_or_id, TableConfig) else int(cfg_or_id)
    table_name = cfg_or_id.table_name if isinstance(cfg_or_id, TableConfig) else None
    # Timestamps travel as UTC strings and are cast by Spark, so the result doesn't depend on
    # the Python process's local time zone.
    row = [(str(run_id), table_id, table_name, layer, str(start_time), str(utc_now()), status,
            None if rows_processed is None else int(rows_processed),
            None if error_message is None else str(error_message)[:4000])]
    schema = ("run_id STRING, table_id INT, table_name STRING, layer STRING, start_time STRING, "
              "end_time STRING, status STRING, rows_processed BIGINT, error_message STRING")
    df = (spark.createDataFrame(row, schema)
          .selectExpr("run_id", "table_id", "table_name", "layer",
                      "CAST(concat(start_time, 'Z') AS TIMESTAMP) AS start_time",
                      "CAST(concat(end_time, 'Z') AS TIMESTAMP) AS end_time",
                      "status", "rows_processed", "error_message"))
    retry_on_conflict(lambda: df.write.mode("append").saveAsTable(f"{catalog}.{config.METADATA}.pipeline_runs"))
