import pytest

from ledgerlake import bronze


def test_jdbc_query():
    assert bronze.jdbc_query("core.branches", None, None) == "(SELECT * FROM core.branches) AS src"
    q = bronze.jdbc_query("core.customers", "updated_at", "2026-09-30 05:07:17")
    assert "WHERE updated_at > CAST('2026-09-30 05:07:17' AS DATETIME2)" in q
    with pytest.raises(ValueError):
        bronze.jdbc_query("core.customers", "updated_at", "2026-01-01' OR 1=1 --")


def test_rows_from_progress_handles_dicts_and_objects():
    class P:
        def __init__(self, n):
            self.numInputRows = n

    assert bronze.rows_from_progress([{"numInputRows": 5}, {"numInputRows": 7}]) == 12
    assert bronze.rows_from_progress([P(3), P(None)]) == 3
    assert bronze.rows_from_progress(None) == 0
