-- gold.channel_daily_summary: one row per Sydney business date, channel and direction.
-- Source timestamps are UTC; dates are converted to Australia/Sydney so a day means a local day.
SELECT
    CAST(from_utc_timestamp(txn_timestamp, 'Australia/Sydney') AS DATE)       AS business_date,
    channel,
    direction,
    count(*)                                                                 AS txn_count,
    count_if(txn_status = 'POSTED')                                          AS posted_count,
    count_if(txn_status = 'DECLINED')                                        AS declined_count,
    count_if(txn_status = 'REVERSED')                                        AS reversed_count,
    CAST(sum(CASE WHEN txn_status = 'POSTED' THEN amount ELSE 0 END) AS DECIMAL(16,2)) AS posted_value_aud,
    CAST(avg(CASE WHEN txn_status = 'POSTED' THEN amount END) AS DECIMAL(12,2))       AS avg_posted_amount_aud,
    round(100 * count_if(txn_status = 'DECLINED') / count(*), 2)             AS decline_rate_pct
FROM {catalog}.silver.transactions
GROUP BY 1, 2, 3
