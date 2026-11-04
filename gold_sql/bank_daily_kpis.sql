-- gold.bank_daily_kpis: one row per Sydney business date with the bank's headline numbers.
WITH txn AS (
    SELECT
        CAST(from_utc_timestamp(t.txn_timestamp, 'Australia/Sydney') AS DATE)               AS business_date,
        count(*)                                                                            AS txn_count,
        count(DISTINCT a.customer_id)                                                       AS active_customers,
        sum(CASE WHEN t.txn_status = 'POSTED' AND t.direction = 'CREDIT' THEN t.amount ELSE 0 END) AS inflows_aud,
        sum(CASE WHEN t.txn_status = 'POSTED' AND t.direction = 'DEBIT'  THEN t.amount ELSE 0 END) AS outflows_aud,
        sum(CASE WHEN t.txn_status = 'POSTED' AND t.channel = 'CARD'     THEN t.amount ELSE 0 END) AS card_spend_aud,
        count_if(t.txn_status = 'DECLINED')                                                 AS declined_count
    FROM {catalog}.silver.transactions t
    JOIN {catalog}.silver.accounts a ON t.account_id = a.account_id
    GROUP BY 1
),
gateway AS (
    SELECT
        CAST(from_utc_timestamp(event_timestamp, 'Australia/Sydney') AS DATE) AS business_date,
        count(*)                                                             AS gateway_events,
        sum(fee_aud)                                                         AS gateway_fees_aud,
        percentile_approx(latency_ms, 0.95)                                  AS gateway_p95_latency_ms
    FROM {catalog}.silver.payment_gateway_events
    GROUP BY 1
),
joined AS (
    SELECT customer_since AS business_date, count(*) AS new_customers
    FROM {catalog}.silver.customers
    GROUP BY customer_since
)
SELECT
    t.business_date,
    t.txn_count,
    t.active_customers,
    CAST(t.inflows_aud AS DECIMAL(16,2))                    AS inflows_aud,
    CAST(t.outflows_aud AS DECIMAL(16,2))                   AS outflows_aud,
    CAST(t.inflows_aud - t.outflows_aud AS DECIMAL(16,2))   AS net_flow_aud,
    CAST(t.card_spend_aud AS DECIMAL(16,2))                 AS card_spend_aud,
    t.declined_count,
    round(100 * t.declined_count / t.txn_count, 2)          AS decline_rate_pct,
    coalesce(j.new_customers, 0)                            AS new_customers,
    coalesce(g.gateway_events, 0)                           AS gateway_events,
    CAST(coalesce(g.gateway_fees_aud, 0) AS DECIMAL(14,2))  AS gateway_fees_aud,
    g.gateway_p95_latency_ms
FROM txn t
LEFT JOIN gateway g ON t.business_date = g.business_date
LEFT JOIN joined j  ON t.business_date = j.business_date
