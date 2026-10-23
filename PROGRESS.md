# Progress
<!-- ledgerlake-day: 1 -->

LedgerLake is built in 10 daily steps. Each day's commit adds code; the checklist under it is the hands-on step that proves it works on Databricks. Tick the boxes and add your notes and numbers as you go.

## Day 1 · Project scaffold · 2026-10-24
**Built:** Python package skeleton (`src/ledgerlake`), shared config (catalog, schemas, secret names, landing paths), dev requirements and pytest config.

**Files:** `.gitignore`, `pyproject.toml`, `requirements-dev.txt`, `src/ledgerlake/__init__.py`, `src/ledgerlake/config.py`

**Hands-on step (do this in Databricks / Azure today):**
- [ ] Sign up for **Databricks Free Edition** (not the 14-day trial) and note your workspace URL
- [ ] Create an **Azure account** (you'll use the Azure SQL Database free offer tomorrow)
- [ ] Optional: create a **Gmail app password** (Google Account > Security > 2-Step Verification > App passwords) for the run-summary email on day 7
- [ ] Read `src/ledgerlake/config.py` and be able to explain the catalog/schema layout: metadata, source, bronze, silver, gold

**Notes / numbers / issues hit:**
-
