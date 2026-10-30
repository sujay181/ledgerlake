"""The table registry: which tables the pipeline loads and how.

This is the seed for the metadata tables. To add a table to the pipeline you add a row
here (or insert it straight into metadata.table_registry); no notebook changes needed.

source_system:
  sqlserver  read over JDBC from the core banking database (source_object = schema.table)
  files      CSV files in the landing volume, read with Auto Loader (source_object = folder)
  silver     a gold table built from silver tables by gold_sql/<table_name>.sql
"""

REGISTRY = [
    # table_id, table_name, source_system, source_object, target_layer, load_order, description
    (1, "branches", "sqlserver", "core.branches", "silver", 1, "Branch list with BSB, state and region"),
    (2, "customers", "sqlserver", "core.customers", "silver", 2, "Customer master data, changes over time"),
    (3, "accounts", "sqlserver", "core.accounts", "silver", 3, "Deposit, card and loan accounts with balances"),
    (4, "transactions", "sqlserver", "core.transactions", "silver", 4, "Posted, declined and reversed transactions"),
    (5, "credit_bureau_reports", "files", "credit_bureau_reports", "silver", 1, "External credit bureau pulls"),
    (6, "payment_gateway_events", "files", "payment_gateway_events", "silver", 2, "Card payment processor events"),
    (11, "customer_360", "silver", None, "gold", 1, "One row per customer: products, balances, activity, credit"),
    (12, "branch_scorecard", "silver", None, "gold", 2, "Branch deposits, lending, growth and risk"),
    (13, "channel_daily_summary", "silver", None, "gold", 3, "Daily transactions by channel and direction"),
    (14, "bank_daily_kpis", "silver", None, "gold", 4, "Headline daily KPIs for the bank"),
    (15, "credit_risk_watchlist", "silver", None, "gold", 5, "Customers flagged for credit risk review"),
    (16, "payment_gateway_daily", "silver", None, "gold", 6, "Gateway volumes, decline rates, latency, fees"),
]

# table_id -> {parameter_name: parameter_value}
PARAMETERS = {
    1: {"load_type": "FULL", "primary_key": "branch_id"},
    2: {"load_type": "MERGE", "primary_key": "customer_id", "watermark_column": "updated_at"},
    3: {"load_type": "MERGE", "primary_key": "account_id", "watermark_column": "updated_at"},
    4: {"load_type": "APPEND", "primary_key": "transaction_id", "watermark_column": "txn_timestamp"},
    5: {"load_type": "MERGE", "primary_key": "customer_id", "watermark_column": "pulled_at",
        "file_format": "csv",
        "schema_hints": "pulled_at TIMESTAMP, credit_score INT, open_credit_accounts INT, "
                        "defaults_last_5y INT, external_debt_aud DECIMAL(14,2)"},
    6: {"load_type": "APPEND", "primary_key": "event_id", "watermark_column": "event_timestamp",
        "file_format": "csv",
        "schema_hints": "event_timestamp TIMESTAMP, response_code STRING, latency_ms INT, fee_aud DECIMAL(10,2)"},
}
