from ledgerlake import notify


def row(layer, table, status, end, rows=10, err=None):
    return {"layer": layer, "table_name": table, "status": status, "rows_processed": rows,
            "start_time": end, "end_time": end, "error_message": err}


def test_latest_attempt_wins_and_order():
    rows = [
        row("silver", "customers", "FAILED", "2026-10-23 01:00", err="boom"),
        row("silver", "customers", "SUCCESS", "2026-10-23 02:00"),
        row("gold", "customer_360", "SUCCESS", "2026-10-23 03:00"),
        row("bronze", "customers", "SUCCESS", "2026-10-23 00:30"),
    ]
    latest = notify.latest_attempts(rows)
    assert [(r["layer"], r["status"]) for r in latest] == [
        ("bronze", "SUCCESS"), ("silver", "SUCCESS"), ("gold", "SUCCESS")]


def test_overall_status():
    ok = [row("bronze", "a", "SUCCESS", "x")]
    assert notify.overall_status(ok) == "SUCCESS"
    assert notify.overall_status(ok, expected_tables=2) == "INCOMPLETE"
    assert notify.overall_status(ok + [row("silver", "a", "FAILED", "y")]) == "FAILED"
    assert notify.overall_status([]) == "FAILED"


def test_html_escapes_error_text():
    html_body = notify.build_html([row("silver", "a", "FAILED", "x", err="<script>x</script>")],
                                  "123", "FAILED", "Coral Digital Bank")
    assert "<script>" not in html_body and "&lt;script&gt;" in html_body
    assert "FAILED" in notify.subject("FAILED", "Coral Digital Bank")
