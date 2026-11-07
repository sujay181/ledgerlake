"""Every dataset in dashboards/dashboard_queries.sql runs against the gold tables."""
import pytest

from pipeline_helpers import ROOT, build_gold, build_silver, create_pipeline_runs


@pytest.fixture(scope="module")
def silver_tables(spark, generated):
    out, counts = generated
    return out, counts, build_silver(spark, out)


def test_dashboard_queries_run(spark, silver_tables):
    """Every dataset in dashboards/dashboard_queries.sql runs against the gold tables."""
    build_gold(spark)
    create_pipeline_runs(spark)
    text = (ROOT / "dashboards" / "dashboard_queries.sql").read_text()
    body = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    queries = [q.strip() for q in body.split(";") if q.strip()]
    assert len(queries) == 8
    for q in queries:
        spark.sql(q.replace("ledgerlake.", "spark_catalog.")).collect()
    kpi = spark.sql(queries[0].replace("ledgerlake.", "spark_catalog.")).first()
    assert kpi.customers == spark.table("silver.customers").count()
