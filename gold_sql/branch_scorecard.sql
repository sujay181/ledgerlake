-- gold.branch_scorecard: one row per branch (customers are assigned to their home branch).
WITH as_of AS (
    SELECT CAST(max(txn_timestamp) AS DATE) AS as_of_date
    FROM {catalog}.silver.transactions
),
customers AS (
    SELECT
        c.home_branch_id                                               AS branch_id,
        count(*)                                                       AS customers,
        count_if(c.customer_since >= date_sub(as_of.as_of_date, 29))   AS new_customers_30d,
        count_if(c.kyc_status <> 'VERIFIED')                           AS kyc_incomplete
    FROM {catalog}.silver.customers c
    CROSS JOIN as_of
    GROUP BY c.home_branch_id
),
balances AS (
    SELECT
        c.home_branch_id                                                     AS branch_id,
        count(*)                                                             AS open_accounts,
        sum(CASE WHEN a.current_balance > 0 THEN a.current_balance ELSE 0 END)  AS deposits_aud,
        sum(CASE WHEN a.current_balance < 0 THEN -a.current_balance ELSE 0 END) AS lending_aud
    FROM {catalog}.silver.accounts a
    JOIN {catalog}.silver.customers c ON a.customer_id = c.customer_id
    WHERE a.account_status = 'OPEN'
    GROUP BY c.home_branch_id
),
credit AS (
    SELECT
        c.home_branch_id                                   AS branch_id,
        avg(cr.credit_score)                               AS avg_credit_score,
        count_if(cr.risk_band IN ('POOR', 'FAIR'))         AS fair_or_poor_credit
    FROM {catalog}.silver.customers c
    JOIN {catalog}.silver.credit_bureau_reports cr ON c.customer_id = cr.customer_id
    GROUP BY c.home_branch_id
)
SELECT
    b.branch_id,
    b.bsb,
    b.branch_name,
    b.city,
    b.state,
    b.region,
    coalesce(cu.customers, 0)                                         AS customers,
    coalesce(cu.new_customers_30d, 0)                                 AS new_customers_30d,
    coalesce(cu.kyc_incomplete, 0)                                    AS kyc_incomplete,
    coalesce(ba.open_accounts, 0)                                     AS open_accounts,
    CAST(coalesce(ba.deposits_aud, 0) AS DECIMAL(16,2))               AS deposits_aud,
    CAST(coalesce(ba.lending_aud, 0) AS DECIMAL(16,2))                AS lending_aud,
    round(try_divide(ba.lending_aud, ba.deposits_aud), 2)             AS loan_to_deposit_ratio,
    CAST(round(cr.avg_credit_score, 0) AS INT)                        AS avg_credit_score,
    round(100 * try_divide(cr.fair_or_poor_credit, cu.customers), 1)  AS pct_fair_or_poor_credit,
    rank() OVER (ORDER BY coalesce(ba.deposits_aud, 0) DESC)          AS deposit_rank,
    as_of.as_of_date
FROM {catalog}.silver.branches b
CROSS JOIN as_of
LEFT JOIN customers cu ON b.branch_id = cu.branch_id
LEFT JOIN balances ba  ON b.branch_id = ba.branch_id
LEFT JOIN credit cr    ON b.branch_id = cr.branch_id
