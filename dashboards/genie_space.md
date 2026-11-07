# Genie space: "Coral Digital Bank analyst"

Genie lets people ask questions in plain English over the gold tables.
In Databricks: **Genie > New** and fill it in as below.

## Tables (trusted assets)
- `ledgerlake.gold.customer_360`
- `ledgerlake.gold.branch_scorecard`
- `ledgerlake.gold.bank_daily_kpis`
- `ledgerlake.gold.channel_daily_summary`
- `ledgerlake.gold.credit_risk_watchlist`
- `ledgerlake.gold.payment_gateway_daily`

## General instructions (paste into Instructions)
```
You answer questions about Coral Digital Bank, a fictional Australian digital bank. All data is synthetic.
- Amounts are in Australian dollars (AUD). Columns ending _aud are dollars.
- business_date is the Sydney local date. as_of_date is the last day with data, not today.
- Deposits = positive balances; lending = loans and card debt, shown as positive numbers.
- "Active customer" = a customer with at least one posted transaction that day.
- Decline rate = declined transactions / all transactions, as a percentage.
- Risk bands from best to worst: EXCELLENT, VERY_GOOD, GOOD, FAIR, POOR.
- A customer is "at risk" if they appear in credit_risk_watchlist; HIGH priority means 3+ signals.
- When a question is about a period, default to the last 30 days of business_date.
- Round money to whole dollars and percentages to one decimal place.
```

## Sample questions (add these so people see what to ask)
1. Which five branches hold the most deposits, and what is their loan-to-deposit ratio?
2. How did daily card spend trend over the last 90 days?
3. Which payment gateway had the highest p95 latency last month?
4. How many HIGH priority customers are on the credit risk watchlist, by state?
5. What share of customers are dormant (no transactions in 30 days) in each value segment?
6. What was the decline rate by channel last month?

## Trusted SQL example (optional, improves answers about deposits by state)
```sql
SELECT state, round(sum(total_deposits_aud)) AS deposits_aud, count(*) AS customers
FROM ledgerlake.gold.customer_360
GROUP BY state
ORDER BY deposits_aud DESC
```
