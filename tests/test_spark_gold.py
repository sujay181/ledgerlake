"""Every gold SQL file, run on local Spark against silver tables built from generated data."""
import pytest

from pipeline_helpers import build_silver, run_gold


@pytest.fixture(scope="module")
def silver_tables(spark, generated):
    out, counts = generated
    return out, counts, build_silver(spark, out)


def test_gold_customer_360(spark, silver_tables):
    df = run_gold(spark, "customer_360")
    n_customers = spark.table("silver.customers").count()
    assert df.count() == n_customers
    assert df.where("customer_id IS NULL OR accounts_held < 1").count() == 0
    deposits = spark.sql("SELECT sum(current_balance) s FROM silver.accounts WHERE current_balance > 0 "
                         "AND account_status = 'OPEN'").first().s
    assert df.agg({"total_deposits_aud": "sum"}).first()[0] == deposits


def test_gold_branch_scorecard(spark, silver_tables):
    df = run_gold(spark, "branch_scorecard")
    assert df.count() == spark.table("silver.branches").count()
    assert df.agg({"customers": "sum"}).first()[0] == spark.table("silver.customers").count()


def test_gold_channel_daily_summary(spark, silver_tables):
    df = run_gold(spark, "channel_daily_summary")
    assert df.agg({"txn_count": "sum"}).first()[0] == spark.table("silver.transactions").count()
    assert df.where("decline_rate_pct < 0 OR decline_rate_pct > 100").count() == 0


def test_gold_bank_daily_kpis(spark, silver_tables):
    df = run_gold(spark, "bank_daily_kpis")
    assert df.agg({"txn_count": "sum"}).first()[0] == spark.table("silver.transactions").count()
    assert df.select("business_date").distinct().count() == df.count()


def test_gold_credit_risk_watchlist(spark, silver_tables):
    df = run_gold(spark, "credit_risk_watchlist")
    assert df.count() > 0
    assert df.where("risk_points < 1 OR reasons = ''").count() == 0
    assert set(r.review_priority for r in df.select("review_priority").distinct().collect()) <= {"LOW", "MEDIUM", "HIGH"}


def test_gold_payment_gateway_daily(spark, silver_tables):
    df = run_gold(spark, "payment_gateway_daily")
    assert df.agg({"events": "sum"}).first()[0] == spark.table("silver.payment_gateway_events").count()
    assert df.agg({"events_missing_latency": "sum"}).first()[0] == 3
    assert df.agg({"events_without_transaction": "sum"}).first()[0] == 0

