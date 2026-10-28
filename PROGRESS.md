# Progress
<!-- ledgerlake-day: 2 -->

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
