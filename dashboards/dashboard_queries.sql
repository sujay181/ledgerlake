-- Datasets for the "Coral Digital Bank · Daily Operations" AI/BI dashboard.
-- In Databricks: Dashboards > Create dashboard > Data tab > "Create from SQL", paste one query
-- per dataset (replace ledgerlake with your catalog), then build the widgets listed under each.

-- ============================================================ 1. kpi_latest
-- Widgets: counter tiles: Customers, Deposits, Lending, Card spend (latest day), Decline rate
SELECT
    (SELECT count(*) FROM ledgerlake.gold.customer_360)                       AS customers,
    (SELECT sum(total_deposits_aud) FROM ledgerlake.gold.customer_360)        AS deposits_aud,
    (SELECT sum(total_lending_aud) FROM ledgerlake.gold.customer_360)         AS lending_aud,
    k.business_date,
    k.card_spend_aud,
    k.decline_rate_pct,
    k.active_customers
FROM ledgerlake.gold.bank_daily_kpis k
ORDER BY k.business_date DESC
LIMIT 1;

-- ============================================================ 2. daily_trend
-- Widgets: line chart of txn_count and active_customers by business_date;
--          bar chart of inflows_aud vs outflows_aud by week
SELECT business_date, txn_count, active_customers, inflows_aud, outflows_aud, net_flow_aud,
       card_spend_aud, decline_rate_pct, new_customers
FROM ledgerlake.gold.bank_daily_kpis
ORDER BY business_date;

-- ============================================================ 3. channel_mix
-- Widgets: stacked bar of posted_value_aud by month and channel; table of decline rate by channel
SELECT date_trunc('MONTH', business_date) AS month, channel, direction,
       sum(txn_count) AS txn_count, sum(posted_value_aud) AS posted_value_aud,
       round(100 * sum(declined_count) / sum(txn_count), 2) AS decline_rate_pct
FROM ledgerlake.gold.channel_daily_summary
GROUP BY 1, 2, 3;

-- ============================================================ 4. branch_scorecard
-- Widgets: table sorted by deposit_rank; bar chart of deposits_aud by state; filter on region
SELECT branch_name, state, region, customers, new_customers_30d, deposits_aud, lending_aud,
       loan_to_deposit_ratio, avg_credit_score, pct_fair_or_poor_credit, deposit_rank
FROM ledgerlake.gold.branch_scorecard;

-- ============================================================ 5. segments
-- Widgets: pie/bar of customers by value_segment; bar of customers by risk_band
SELECT value_segment, risk_band, state, count(*) AS customers,
       sum(total_deposits_aud) AS deposits_aud, sum(total_lending_aud) AS lending_aud,
       count_if(is_dormant_30d) AS dormant_customers
FROM ledgerlake.gold.customer_360
GROUP BY 1, 2, 3;

-- ============================================================ 6. risk_watchlist
-- Widgets: table filtered to review_priority IN ('HIGH','MEDIUM'); counter of HIGH
SELECT review_priority, customer_id, customer_name, state, credit_score, risk_band,
       card_utilisation_pct, declines_30d, total_lending_aud, reasons
FROM ledgerlake.gold.credit_risk_watchlist
ORDER BY risk_points DESC, total_lending_aud DESC;

-- ============================================================ 7. gateway_health
-- Widgets: line of p95_latency_ms by date per gateway; bar of decline_rate_pct per gateway
SELECT business_date, gateway, events, decline_rate_pct, p50_latency_ms, p95_latency_ms,
       settled_value_aud, fees_aud
FROM ledgerlake.gold.payment_gateway_daily
ORDER BY business_date;

-- ============================================================ 8. pipeline_health
-- Widgets: table of the latest run per table and layer; counter of failures in the last 7 days
SELECT layer, table_name, status, rows_processed, start_time, end_time, error_message
FROM (
    SELECT *, row_number() OVER (PARTITION BY layer, table_name ORDER BY start_time DESC) AS rn
    FROM ledgerlake.metadata.pipeline_runs
)
WHERE rn = 1
ORDER BY layer, table_name;
