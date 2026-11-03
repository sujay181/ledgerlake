-- gold.customer_360: one row per customer.
-- Products and balances (open accounts), activity in the last 90 days, latest credit pull.
-- as_of_date is the last day with transactions, so the numbers don't drift as the calendar moves.
WITH as_of AS (
    SELECT CAST(max(txn_timestamp) AS DATE) AS as_of_date
    FROM {catalog}.silver.transactions
),
holdings AS (
    SELECT
        customer_id,
        count(*)                                                        AS accounts_held,
        count(DISTINCT product_type)                                    AS products_held,
        array_join(sort_array(collect_set(product_type)), ', ')         AS products,
        sum(CASE WHEN current_balance > 0 THEN current_balance ELSE 0 END)  AS total_deposits_aud,
        sum(CASE WHEN current_balance < 0 THEN -current_balance ELSE 0 END) AS total_lending_aud,
        sum(current_balance)                                            AS net_position_aud
    FROM {catalog}.silver.accounts
    WHERE account_status = 'OPEN'
    GROUP BY customer_id
),
activity AS (
    SELECT
        a.customer_id,
        count(*)                                                                         AS txns_90d,
        sum(CASE WHEN t.channel = 'CARD' AND t.direction = 'DEBIT' THEN t.amount ELSE 0 END) AS card_spend_90d_aud,
        max(t.txn_timestamp)                                                             AS last_txn_at
    FROM {catalog}.silver.transactions t
    JOIN {catalog}.silver.accounts a ON t.account_id = a.account_id
    CROSS JOIN as_of
    WHERE t.txn_status = 'POSTED'
      AND t.txn_timestamp >= date_sub(as_of.as_of_date, 89)
    GROUP BY a.customer_id
)
SELECT
    c.customer_id,
    concat(c.first_name, ' ', c.last_name)                                AS customer_name,
    CAST(floor(months_between(as_of.as_of_date, c.date_of_birth) / 12) AS INT) AS age,
    c.state,
    c.postcode,
    b.branch_name                                                         AS home_branch,
    b.region,
    c.kyc_status,
    c.customer_since,
    round(months_between(as_of.as_of_date, c.customer_since) / 12, 1)     AS tenure_years,
    coalesce(h.accounts_held, 0)                                          AS accounts_held,
    coalesce(h.products_held, 0)                                          AS products_held,
    h.products,
    CAST(coalesce(h.total_deposits_aud, 0) AS DECIMAL(16,2))              AS total_deposits_aud,
    CAST(coalesce(h.total_lending_aud, 0) AS DECIMAL(16,2))               AS total_lending_aud,
    CAST(coalesce(h.net_position_aud, 0) AS DECIMAL(16,2))                AS net_position_aud,
    coalesce(act.txns_90d, 0)                                             AS txns_90d,
    CAST(coalesce(act.card_spend_90d_aud, 0) AS DECIMAL(16,2))            AS card_spend_90d_aud,
    act.last_txn_at,
    cr.credit_score,
    cr.risk_band,
    cr.pulled_at                                                          AS credit_pulled_at,
    CASE
        WHEN coalesce(h.total_deposits_aud, 0) >= 100000 THEN 'PREMIUM'
        WHEN coalesce(h.total_deposits_aud, 0) >= 20000  THEN 'GROWTH'
        ELSE 'EVERYDAY'
    END                                                                   AS value_segment,
    (act.last_txn_at IS NULL OR act.last_txn_at < date_sub(as_of.as_of_date, 29)) AS is_dormant_30d,
    as_of.as_of_date
FROM {catalog}.silver.customers c
CROSS JOIN as_of
LEFT JOIN {catalog}.silver.branches b              ON c.home_branch_id = b.branch_id
LEFT JOIN holdings h                               ON c.customer_id = h.customer_id
LEFT JOIN activity act                             ON c.customer_id = act.customer_id
LEFT JOIN {catalog}.silver.credit_bureau_reports cr ON c.customer_id = cr.customer_id
