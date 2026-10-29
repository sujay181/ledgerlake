"""Load generated CSV batches into the SQL Server source database.

    pip install pyodbc
    # Windows also needs "ODBC Driver 18 for SQL Server" from Microsoft

    set LL_SQL_SERVER=your-server.database.windows.net
    set LL_SQL_DATABASE=coralbank
    set LL_SQL_USER=your_admin_user
    set LL_SQL_PASSWORD=...

    python load_sqlserver.py --init  generated/sqlserver/batch_01_historical
    python load_sqlserver.py         generated/sqlserver/batch_02_incremental

--init (re)creates the tables first. Branches, customers and accounts are upserted by
primary key (UPDATE existing rows, INSERT new ones), so loading batch 02 changes rows the
way a core banking system would. Transactions are insert-only.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
from pathlib import Path

LOAD_ORDER = ["branches", "customers", "accounts", "transactions"]
PRIMARY_KEYS = {"branches": "branch_id", "customers": "customer_id", "accounts": "account_id",
                "transactions": "transaction_id"}
UPSERT_TABLES = {"customers", "accounts", "branches"}
BATCH = 5_000


def connect():
    import pyodbc  # imported here so the module can be imported without the driver installed

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER=tcp:{os.environ['LL_SQL_SERVER']},1433;"
        f"DATABASE={os.environ['LL_SQL_DATABASE']};"
        f"UID={os.environ['LL_SQL_USER']};PWD={os.environ['LL_SQL_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def split_go_batches(sql: str) -> list[str]:
    """Split a T-SQL script on GO separators (pyodbc can't run GO)."""
    return [b.strip() for b in re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE | re.IGNORECASE) if b.strip()]


def read_rows(path: Path) -> tuple[list[str], list[tuple]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = [tuple(None if v == "" else v for v in r) for r in reader]
    return header, rows


def split_existing(rows: list[tuple], pk_idx: int, existing_ids: set) -> tuple[list[tuple], list[tuple]]:
    """Separate rows whose primary key already exists (to UPDATE) from new rows (to INSERT)."""
    updates = [r for r in rows if r[pk_idx] in existing_ids]
    inserts = [r for r in rows if r[pk_idx] not in existing_ids]
    return updates, inserts


def load_table(cur, table: str, path: Path) -> tuple[int, int]:
    header, rows = read_rows(path)
    pk = PRIMARY_KEYS[table]
    pk_idx = header.index(pk)
    updates: list[tuple] = []
    inserts = rows
    if table in UPSERT_TABLES:
        existing = {r[0] for r in cur.execute(f"SELECT {pk} FROM core.{table}").fetchall()}
        updates, inserts = split_existing(rows, pk_idx, existing)

    cur.fast_executemany = True
    if updates:
        # UPDATE ... SET every non-key column, WHERE key = ?  (keeps foreign keys intact)
        set_cols = [c for c in header if c != pk]
        sql = f"UPDATE core.{table} SET {', '.join(f'{c} = ?' for c in set_cols)} WHERE {pk} = ?"
        params = [tuple(r[header.index(c)] for c in set_cols) + (r[pk_idx],) for r in updates]
        for i in range(0, len(params), BATCH):
            cur.executemany(sql, params[i:i + BATCH])
    if inserts:
        sql = f"INSERT INTO core.{table} ({', '.join(header)}) VALUES ({', '.join('?' for _ in header)})"
        for i in range(0, len(inserts), BATCH):
            cur.executemany(sql, inserts[i:i + BATCH])
    return len(updates), len(inserts)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("batch_dir", type=Path)
    p.add_argument("--init", action="store_true", help="drop and create the core tables first")
    a = p.parse_args()

    conn = connect()
    cur = conn.cursor()
    if a.init:
        ddl = (Path(__file__).parent / "sqlserver" / "01_create_tables.sql").read_text()
        for stmt in split_go_batches(ddl):
            cur.execute(stmt)
        conn.commit()
        print("Created core tables")

    for table in LOAD_ORDER:
        path = a.batch_dir / f"{table}.csv"
        if not path.exists():
            continue
        updated, inserted = load_table(cur, table, path)
        conn.commit()
        print(f"core.{table:<13} {inserted:>9,} inserted {updated:>7,} updated")
    conn.close()


if __name__ == "__main__":
    main()
