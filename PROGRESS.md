# Progress
<!-- ledgerlake-day: 12 -->

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

## Day 6 · Bronze ingestion · 2026-11-02
**Built:** Notebooks to list the registered tables, read each table's load parameters and load bronze from SQL Server or the landing volume.

**Files:** `notebooks/ingestion/01_list_tables.py`, `notebooks/ingestion/02_read_table_parameters.py`, `notebooks/ingestion/03_source_to_bronze.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Pull the Git folder and run `notebooks/ingestion/01_list_tables` and `02_read_table_parameters`: check they match `metadata.table_registry`
- [ ] Upload the two `*_batch_01.csv` files to `/Volumes/<catalog>/source/landing/<folder>/`
- [ ] Run `notebooks/ingestion/03_source_to_bronze` with its default widgets (customers), then again for `payment_gateway_events` (table_id 6, source_system files)
- [ ] Check `bronze.customers`, `bronze.payment_gateway_events` and `metadata.pipeline_runs`. Find the 3 rows with `_rescued_data`

**Notes / numbers / issues hit:**
-

## Day 7 · Silver layer · 2026-11-03
**Built:** Bronze-to-silver: slice by bronze watermark (late files still processed), keep the newest row per key, FULL overwrite / insert-only APPEND / newer-wins MERGE; local Spark tests on generated data.

**Files:** `src/ledgerlake/silver.py`, `notebooks/ingestion/04_bronze_to_silver.py`, `tests/pipeline_helpers.py`, `tests/test_silver.py`, `tests/test_spark_silver.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Pull the Git folder. For all 6 tables run `03_source_to_bronze` then `04_bronze_to_silver` (change the two widgets per table, copying from `metadata.table_registry` / `table_parameters`)
- [ ] Check silver has one row per primary key: `SELECT customer_id, COUNT(*) FROM silver.customers GROUP BY 1 HAVING COUNT(*) > 1` should return nothing
- [ ] Look at `metadata.table_watermarks`: explain the difference between `source_watermark` and `bronze_watermark`

**Notes / numbers / issues hit:**
-

## Day 8 · Gold layer, part 1 · 2026-11-04
**Built:** Gold driver (SQL files rebuilt with CREATE OR REPLACE) and the first three gold tables: customer_360, branch_scorecard, channel_daily_summary.

**Files:** `src/ledgerlake/gold.py`, `notebooks/gold/01_build_gold_table.py`, `gold_sql/customer_360.sql`, `gold_sql/branch_scorecard.sql`, `gold_sql/channel_daily_summary.sql`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Pull the Git folder and run `notebooks/gold/01_build_gold_table` for table_id 11, 12 and 13
- [ ] Sanity-check: total deposits in `gold.customer_360` equal the positive balances in `silver.accounts`

**Notes / numbers / issues hit:**
-

## Day 9 · Gold layer, part 2 · 2026-11-05
**Built:** The remaining gold tables (bank_daily_kpis, credit_risk_watchlist, payment_gateway_daily) and local Spark tests of every gold query.

**Files:** `gold_sql/bank_daily_kpis.sql`, `gold_sql/credit_risk_watchlist.sql`, `gold_sql/payment_gateway_daily.sql`, `tests/test_gold.py`, `tests/test_spark_gold.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Pull the Git folder and run `notebooks/gold/01_build_gold_table` for table_id 14, 15 and 16
- [ ] Pick one finding you could say in an interview (e.g. which gateway has the worst p95 latency)

**Notes / numbers / issues hit:**
-

## Day 10 · Run summary email · 2026-11-06
**Built:** HTML run summary from metadata.pipeline_runs (latest attempt per table, overall SUCCESS / FAILED / INCOMPLETE) sent with Gmail SMTP; HTML escaping of error text.

**Files:** `src/ledgerlake/notify.py`, `notebooks/notify/01_send_run_summary.py`, `tests/test_notify.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Store your Gmail app password with `setup/01_create_secrets` (gmail_app_password widget only)
- [ ] Run `notebooks/notify/01_send_run_summary` with run_id `manual`, sender = your Gmail, recipients = your email
- [ ] Check the email arrives and the table matches `metadata.pipeline_runs`

**Notes / numbers / issues hit:**
-

## Day 11 · Jobs as code · 2026-11-07
**Built:** Databricks Asset Bundle with four jobs: per-table ingest, per-source for-each, gold for-each and a master job with one run_id, a run-if-all-done email task and a paused daily schedule.

**Files:** `databricks.yml`, `resources/jobs.yml`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Install the Databricks CLI (`winget install Databricks.DatabricksCLI`) and run `databricks auth login --host <workspace-url>`
- [ ] From the repo root: `databricks bundle validate`, then `databricks bundle deploy --var="summary_recipients=<you>" --var="gmail_sender=<you>@gmail.com"`
- [ ] Run **ll_master** from Jobs & Pipelines and watch the for-each tasks; check the email
- [ ] If anything fails, write down the error and the fix; it's an interview story

**Notes / numbers / issues hit:**
-

## Day 12 · Checks, dashboard and Genie · 2026-11-08
**Built:** Admin notebook (runs, watermarks, row counts by layer, rescued rows); eight dashboard datasets with widget notes; Genie space instructions and sample questions; test that every dashboard query runs.

**Files:** `notebooks/admin/check_pipeline.py`, `dashboards/dashboard_queries.sql`, `dashboards/genie_space.md`, `tests/test_dashboards.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Load the incremental batch: `python source_data/load_sqlserver.py source_data/generated/sqlserver/batch_02_incremental` and upload the two `*_batch_02.csv` files
- [ ] Run **ll_master** again, then `notebooks/admin/check_pipeline`: only new/changed rows read, duplicate credit pulls and repeated gateway events handled
- [ ] Run ll_master a third time with no new data: it should load 0 rows
- [ ] Build the AI/BI dashboard from `dashboards/dashboard_queries.sql` and the Genie space from `dashboards/genie_space.md`

**Notes / numbers / issues hit:**
-
