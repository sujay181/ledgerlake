# LedgerLake · metadata-driven lakehouse for a digital bank

> **Building in public: day 11 of 13.** Each day adds one layer of the pipeline and the
> Databricks step that proves it works. See [PROGRESS.md](PROGRESS.md). Full documentation lands on day 13.

LedgerLake is an end-to-end data engineering project on **Databricks Free Edition** for Coral Digital Bank, a fictional Australian neobank (all data is synthetic). It loads core-banking tables from **SQL Server** and partner files from a **landing volume** into **bronze, silver and gold** layers in **Unity Catalog**. Metadata tables decide what loads and how (FULL / APPEND / MERGE, keys, watermarks), so four generic notebooks load every table. It runs on **Databricks Jobs**, emails a run summary, and serves an **AI/BI dashboard** and a **Genie** space.

## Built so far
- **Day 1 · Project scaffold**: Python package skeleton (`src/ledgerlake`), shared config (catalog, schemas, secret names, landing paths), dev requirements and pytest config.
- **Day 2 · Data generator**: Data generator for Coral Digital Bank (branches, customers, accounts, transactions, credit bureau reports, payment gateway events) with a historical and an incremental batch and deliberate data issues; generator tests; GitHub Actions CI.
- **Day 3 · SQL Server source**: SQL Server DDL for the core-banking schema and a pyodbc loader with upserts for the historical and incremental batches.
- **Day 4 · Metadata framework**: Table registry and load parameters for 6 source tables and 6 gold tables; config validation and SQL identifier guards; watermark and append-only audit helpers; setup notebook that creates the catalog, schemas, landing volume and metadata tables.
- **Day 5 · Bronze helpers and secrets**: Source-to-bronze helpers for SQL Server (JDBC, incremental by source watermark) and landing files (Auto Loader, availableNow, schema hints, rescued data); secrets setup with the Databricks SDK and a JDBC connectivity check.
- **Day 6 · Bronze ingestion**: Notebooks to list the registered tables, read each table's load parameters and load bronze from SQL Server or the landing volume.
- **Day 7 · Silver layer**: Bronze-to-silver: slice by bronze watermark (late files still processed), keep the newest row per key, FULL overwrite / insert-only APPEND / newer-wins MERGE; local Spark tests on generated data.
- **Day 8 · Gold layer, part 1**: Gold driver (SQL files rebuilt with CREATE OR REPLACE) and the first three gold tables: customer_360, branch_scorecard, channel_daily_summary.
- **Day 9 · Gold layer, part 2**: The remaining gold tables (bank_daily_kpis, credit_risk_watchlist, payment_gateway_daily) and local Spark tests of every gold query.
- **Day 10 · Run summary email**: HTML run summary from metadata.pipeline_runs (latest attempt per table, overall SUCCESS / FAILED / INCOMPLETE) sent with Gmail SMTP; HTML escaping of error text.
- **Day 11 · Jobs as code**: Databricks Asset Bundle with four jobs: per-table ingest, per-source for-each, gold for-each and a master job with one run_id, a run-if-all-done email task and a paused daily schedule.

## Coming next
- Day 12 · Checks, dashboard and Genie
- Day 13 · Documentation

## Stack
Databricks Free Edition · Unity Catalog · Delta Lake · PySpark / Spark SQL · Auto Loader · Databricks Jobs · Asset Bundles · Azure SQL Database · GitHub Actions · pytest

## Credits
The architecture follows DataBeli's tutorial *Finance Domain End-to-End Data Engineering Project in Databricks (Free Edition)*. The code, data model, data generator, gold logic and tests in this repo are written from scratch.
