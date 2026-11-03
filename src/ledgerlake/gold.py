"""Silver -> gold. Each gold table is one SELECT in gold_sql/<table_name>.sql.

The SQL files use {catalog} as a placeholder so the same files work in any catalog.
The driver wraps the SELECT in CREATE OR REPLACE TABLE, so a gold table is always a
complete rebuild from silver: re-running is safe and there is no gold state to repair.
"""
from __future__ import annotations

import re
from pathlib import Path

from . import config
from .metadata import check_identifier

_PLACEHOLDER = re.compile(r"\{catalog\}")


def render(sql_text: str, catalog: str) -> str:
    check_identifier(catalog)
    sql = _PLACEHOLDER.sub(catalog, sql_text).strip().rstrip(";").strip()
    if "{" in sql.replace("{catalog}", ""):
        raise ValueError("Unknown placeholder in gold SQL (only {catalog} is supported)")
    return sql


def load_sql(sql_dir: str | Path, table_name: str) -> str:
    check_identifier(table_name)
    path = Path(sql_dir) / f"{table_name}.sql"
    if not path.exists():
        raise FileNotFoundError(f"No gold SQL file for {table_name}: {path}")
    return path.read_text(encoding="utf-8")


def build_table(spark, catalog: str, table_name: str, sql_dir: str | Path) -> int:
    select_sql = render(load_sql(sql_dir, table_name), catalog)
    target = config.fqn(catalog, config.GOLD, table_name)
    spark.sql(f"CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
    return int(spark.table(target).count())
