import csv


def read(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_files_and_counts(generated):
    out, counts = generated
    b1 = out / "sqlserver" / "batch_01_historical"
    assert len(read(b1 / "customers.csv")) == counts["customers_batch_01"] == 150
    assert len(read(b1 / "transactions.csv")) == counts["transactions_batch_01"] > 1000
    for name in ("credit_bureau_reports", "payment_gateway_events"):
        for batch in ("01", "02"):
            assert (out / "landing" / name / f"{name}_batch_{batch}.csv").exists()


def test_referential_integrity(generated):
    out, _ = generated
    b1, b2 = out / "sqlserver" / "batch_01_historical", out / "sqlserver" / "batch_02_incremental"
    branches = {r["branch_id"] for r in read(b1 / "branches.csv")}
    customers = {r["customer_id"]: r for r in read(b1 / "customers.csv")}
    customers.update({r["customer_id"]: r for r in read(b2 / "customers.csv")})
    accounts = {r["account_id"]: r for r in read(b1 / "accounts.csv")}
    accounts.update({r["account_id"]: r for r in read(b2 / "accounts.csv")})
    assert all(c["home_branch_id"] in branches for c in customers.values())
    assert all(a["customer_id"] in customers for a in accounts.values())
    for f in (b1 / "transactions.csv", b2 / "transactions.csv"):
        assert all(t["account_id"] in accounts for t in read(f))


def test_incremental_batch_is_newer(generated):
    out, _ = generated
    t1 = read(out / "sqlserver" / "batch_01_historical" / "transactions.csv")
    t2 = read(out / "sqlserver" / "batch_02_incremental" / "transactions.csv")
    assert min(t["txn_timestamp"] for t in t2) > max(t["txn_timestamp"] for t in t1)
    ids1 = {t["transaction_id"] for t in t1}
    assert not ids1 & {t["transaction_id"] for t in t2}


def test_batch_02_contains_updates_and_new_customers(generated):
    out, _ = generated
    c1 = {r["customer_id"] for r in read(out / "sqlserver" / "batch_01_historical" / "customers.csv")}
    c2 = [r["customer_id"] for r in read(out / "sqlserver" / "batch_02_incremental" / "customers.csv")]
    assert any(c in c1 for c in c2), "expected updates to existing customers"
    assert any(c not in c1 for c in c2), "expected brand-new customers"


def test_deliberate_data_issues(generated):
    out, _ = generated
    land = out / "landing"
    credit2 = read(land / "credit_bureau_reports" / "credit_bureau_reports_batch_02.csv")
    ids = [r["customer_id"] for r in credit2]
    assert len(ids) > len(set(ids)), "some customers should be pulled twice in batch 02"
    events2 = read(land / "payment_gateway_events" / "payment_gateway_events_batch_02.csv")
    eids = [r["event_id"] for r in events2]
    assert len(eids) > len(set(eids)), "some gateway events should be repeated in batch 02"
    events1 = read(land / "payment_gateway_events" / "payment_gateway_events_batch_01.csv")
    assert sum(r["latency_ms"] == "timeout" for r in events1) == 3


def test_amounts_positive_and_statuses_valid(generated):
    out, _ = generated
    for t in read(out / "sqlserver" / "batch_01_historical" / "transactions.csv"):
        assert float(t["amount"]) > 0
        assert t["direction"] in ("DEBIT", "CREDIT")
        assert t["txn_status"] in ("POSTED", "DECLINED", "REVERSED")


def test_deterministic(tmp_path):
    from datetime import date
    from generate_data import generate

    a = generate(tmp_path / "a", 1, 30, date(2025, 10, 1), 10, 3)
    b = generate(tmp_path / "b", 1, 30, date(2025, 10, 1), 10, 3)
    assert a == b
    assert (tmp_path / "a/sqlserver/batch_01_historical/transactions.csv").read_text() == \
           (tmp_path / "b/sqlserver/batch_01_historical/transactions.csv").read_text()
