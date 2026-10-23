# LedgerLake · metadata-driven lakehouse for a digital bank

> **Building in public: day 1 of 10.** Each day adds one layer of the pipeline and the
> Databricks step that proves it works. See [PROGRESS.md](PROGRESS.md). Full documentation lands on day 10.

LedgerLake is an end-to-end data engineering project on **Databricks Free Edition** for Coral Digital Bank, a fictional Australian neobank (all data is synthetic). It loads core-banking tables from **SQL Server** and partner files from a **landing volume** into **bronze, silver and gold** layers in **Unity Catalog**. Metadata tables decide what loads and how (FULL / APPEND / MERGE, keys, watermarks), so four generic notebooks load every table. It runs on **Databricks Jobs**, emails a run summary, and serves an **AI/BI dashboard** and a **Genie** space.

## Built so far
- **Day 1 · Project scaffold**: Python package skeleton (`src/ledgerlake`), shared config (catalog, schemas, secret names, landing paths), dev requirements and pytest config.

## Coming next
- Day 2 · Source data
- Day 3 · Metadata framework
- Day 4 · Bronze ingestion
- Day 5 · Silver layer
- Day 6 · Gold layer
- Day 7 · Run summary email
- Day 8 · Jobs as code
- Day 9 · Checks, dashboard and Genie
- Day 10 · Documentation

## Stack
Databricks Free Edition · Unity Catalog · Delta Lake · PySpark / Spark SQL · Auto Loader · Databricks Jobs · Asset Bundles · Azure SQL Database · GitHub Actions · pytest

## Credits
The architecture follows DataBeli's tutorial *Finance Domain End-to-End Data Engineering Project in Databricks (Free Edition)*. The code, data model, data generator, gold logic and tests in this repo are written from scratch.
