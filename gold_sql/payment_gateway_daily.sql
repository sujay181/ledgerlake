-- gold.payment_gateway_daily: one row per Sydney business date and gateway.
-- latency_ms is NULL where the source sent a non-numeric value (kept in bronze _rescued_data).
SELECT
    CAST(from_utc_timestamp(e.event_timestamp, 'Australia/Sydney') AS DATE)   AS business_date,
    e.gateway,
    count(*)                                                                 AS events,
    count_if(e.event_status = 'SETTLED')                                     AS settled,
    count_if(e.event_status = 'DECLINED')                                    AS declined,
    count_if(e.event_status = 'REVERSED')                                    AS reversed,
    round(100 * count_if(e.event_status = 'DECLINED') / count(*), 2)         AS decline_rate_pct,
    CAST(sum(CASE WHEN e.event_status = 'SETTLED' THEN t.amount ELSE 0 END) AS DECIMAL(16,2)) AS settled_value_aud,
    CAST(sum(e.fee_aud) AS DECIMAL(14,2))                                    AS fees_aud,
    percentile_approx(e.latency_ms, 0.5)                                     AS p50_latency_ms,
    percentile_approx(e.latency_ms, 0.95)                                    AS p95_latency_ms,
    count_if(e.latency_ms IS NULL)                                           AS events_missing_latency,
    count_if(t.transaction_id IS NULL)                                       AS events_without_transaction
FROM {catalog}.silver.payment_gateway_events e
LEFT JOIN {catalog}.silver.transactions t ON e.transaction_id = t.transaction_id
GROUP BY 1, 2
