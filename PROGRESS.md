# Progress
<!-- ledgerlake-day: 5 -->

LedgerLake is built in 13 daily steps. Each day's commit adds code; the checklist under it is the hands-on step that proves it works on Databricks. Tick the boxes and add your notes and numbers as you go.

## Day 1 · Project scaffold · 2026-10-24
**Built:** Python package skeleton (`src/ledgerlake`), shared config (catalog, schemas, secret names, landing paths), dev requirements and pytest config.

**Files:** `.gitignore`, `pyproject.toml`, `requirements-dev.txt`, `src/ledgerlake/__init__.py`, `src/ledgerlake/config.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Sign up for **Databricks Free Edition** (not the 14-day trial) and note your workspace URL
- [ ] Create an **Azure account** (you'll use the Azure SQL Database free offer on day 3)
- [ ] Optional: create a **Gmail app password** (Google Account > Security > 2-Step Verification > App passwords) for the run-summary email on day 10
- [ ] Read `src/ledgerlake/config.py` and be able to explain the catalog/schema layout: metadata, source, bronze, silver, gold

**Notes / numbers / issues hit:**
-

## Day 2 · Data generator · 2026-10-28
**Built:** Data generator for Coral Digital Bank (branches, customers, accounts, transactions, credit bureau reports, payment gateway events) with a historical and an incremental batch and deliberate data issues; generator tests; GitHub Actions CI.

**Files:** `source_data/generate_data.py`, `tests/conftest.py`, `tests/test_generator.py`, `.github/workflows/ci.yml`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Run `python source_data/generate_data.py --out source_data/generated` and note the row counts it prints
- [ ] Open the generated CSVs and find one of the deliberate data issues (duplicates, nulls, late rows)

**Notes / numbers / issues hit:**
-

## Day 3 · SQL Server source · 2026-10-30
**Built:** SQL Server DDL for the core-banking schema and a pyodbc loader with upserts for the historical and incremental batches.

**Files:** `source_data/load_sqlserver.py`, `source_data/sqlserver/01_create_tables.sql`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Create an **Azure SQL Database** with the free offer (database `coralbank`, auto-pause when the free limit is reached)
- [ ] Add firewall rules: your PC's IP, plus `0.0.0.0 - 255.255.255.255` for Databricks serverless (fake data, strong password)
- [ ] Install ODBC Driver 18 + `pip install pyodbc`, set the LL_SQL_* variables and run `python source_data/load_sqlserver.py --init source_data/generated/sqlserver/batch_01_historical`
- [ ] In the Azure query editor run `SELECT COUNT(*) FROM core.transactions` and check it matches the generator

**Notes / numbers / issues hit:**
-

## Day 4 · Metadata framework · 2026-10-31
**Built:** Table registry and load parameters for 6 source tables and 6 gold tables; config validation and SQL identifier guards; watermark and append-only audit helpers; setup notebook that creates the catalog, schemas, landing volume and metadata tables.

**Files:** `src/ledgerlake/registry.py`, `src/ledgerlake/metadata.py`, `setup/00_create_catalog_and_metadata.py`, `tests/test_metadata.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] In Databricks: **Workspace > Create > Git folder** and paste this repo's URL
- [ ] Run `setup/00_create_catalog_and_metadata` (if creating a catalog fails, use `workspace` as the catalog)
- [ ] Open Catalog Explorer and look at `metadata.table_registry` and `metadata.table_parameters`
- [ ] Be able to explain: why FULL for branches, MERGE for customers, APPEND for transactions?

**Notes / numbers / issues hit:**
-

## Day 5 · Bronze helpers and secrets · 2026-11-01
**Built:** Source-to-bronze helpers for SQL Server (JDBC, incremental by source watermark) and landing files (Auto Loader, availableNow, schema hints, rescued data); secrets setup with the Databricks SDK and a JDBC connectivity check.

**Files:** `src/ledgerlake/bronze.py`, `tests/test_bronze.py`, `setup/01_create_secrets.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Pull the Git folder, run `setup/01_create_secrets`; the last cell must show the customer count from SQL Server. Then remove the widgets and clear outputs

**Notes / numbers / issues hit:**
-
