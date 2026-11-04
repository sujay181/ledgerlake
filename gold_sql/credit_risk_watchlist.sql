-- gold.credit_risk_watchlist: customers with at least one credit-risk signal, with the reasons.
-- Signals: poor bureau score, recent defaults, card above 90% of limit, 3+ declines in 30 days,
-- external debt above $75k.
WITH as_of AS (
    SELECT CAST(max(txn_timestamp) AS DATE) AS as_of_date
    FROM {catalog}.silver.transactions
),
cards AS (
    SELECT customer_id,
           sum(CASE WHEN current_balance < 0 THEN -current_balance ELSE 0 END) AS card_balance_aud,
           sum(credit_limit)                                                    AS card_limit_aud
    FROM {catalog}.silver.accounts
    WHERE product_type = 'CREDIT_CARD' AND account_status = 'OPEN'
    GROUP BY customer_id
),
lending AS (
    SELECT customer_id,
           sum(CASE WHEN current_balance < 0 THEN -current_balance ELSE 0 END) AS total_lending_aud
    FROM {catalog}.silver.accounts
    WHERE account_status = 'OPEN'
    GROUP BY customer_id
),
declines AS (
    SELECT a.customer_id, count(*) AS declines_30d
    FROM {catalog}.silver.transactions t
    JOIN {catalog}.silver.accounts a ON t.account_id = a.account_id
    CROSS JOIN as_of
    WHERE t.txn_status = 'DECLINED' AND t.txn_timestamp >= date_sub(as_of.as_of_date, 29)
    GROUP BY a.customer_id
),
signals AS (
    SELECT
        c.customer_id,
        concat(c.first_name, ' ', c.last_name)                                AS customer_name,
        c.state,
        c.home_branch_id,
        cr.credit_score,
        cr.risk_band,
        cr.defaults_last_5y,
        cr.external_debt_aud,
        CAST(coalesce(l.total_lending_aud, 0) AS DECIMAL(16,2))               AS total_lending_aud,
        round(100 * try_divide(cd.card_balance_aud, cd.card_limit_aud), 1)    AS card_utilisation_pct,
        coalesce(d.declines_30d, 0)                                           AS declines_30d,
        CASE WHEN cr.risk_band = 'POOR' THEN 1 ELSE 0 END                     AS s_poor_score,
        CASE WHEN cr.defaults_last_5y > 0 THEN 1 ELSE 0 END                   AS s_defaults,
        CASE WHEN try_divide(cd.card_balance_aud, cd.card_limit_aud) >= 0.9 THEN 1 ELSE 0 END AS s_card_maxed,
        CASE WHEN coalesce(d.declines_30d, 0) >= 3 THEN 1 ELSE 0 END          AS s_declines,
        CASE WHEN cr.external_debt_aud > 75000 THEN 1 ELSE 0 END              AS s_external_debt,
        as_of.as_of_date
    FROM {catalog}.silver.customers c
    CROSS JOIN as_of
    LEFT JOIN {catalog}.silver.credit_bureau_reports cr ON c.customer_id = cr.customer_id
    LEFT JOIN cards cd    ON c.customer_id = cd.customer_id
    LEFT JOIN lending l   ON c.customer_id = l.customer_id
    LEFT JOIN declines d  ON c.customer_id = d.customer_id
)
SELECT
    customer_id,
    customer_name,
    state,
    home_branch_id,
    credit_score,
    risk_band,
    defaults_last_5y,
    external_debt_aud,
    total_lending_aud,
    card_utilisation_pct,
    declines_30d,
    s_poor_score + s_defaults + s_card_maxed + s_declines + s_external_debt AS risk_points,
    CASE
        WHEN s_poor_score + s_defaults + s_card_maxed + s_declines + s_external_debt >= 3 THEN 'HIGH'
        WHEN s_poor_score + s_defaults + s_card_maxed + s_declines + s_external_debt = 2  THEN 'MEDIUM'
        ELSE 'LOW'
    END AS review_priority,
    concat_ws('; ',
        CASE WHEN s_poor_score = 1    THEN 'Poor bureau score' END,
        CASE WHEN s_defaults = 1      THEN 'Defaults in last 5 years' END,
        CASE WHEN s_card_maxed = 1    THEN 'Card at 90%+ of limit' END,
        CASE WHEN s_declines = 1      THEN '3+ declined payments in 30 days' END,
        CASE WHEN s_external_debt = 1 THEN 'External debt over $75k' END
    ) AS reasons,
    as_of_date
FROM signals
WHERE s_poor_score + s_defaults + s_card_maxed + s_declines + s_external_debt > 0
