"""End-of-run email: a summary of metadata.pipeline_runs for one run, sent through Gmail SMTP."""
from __future__ import annotations

import html
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

COLUMNS = ["layer", "table_name", "status", "rows_processed", "start_time", "end_time", "error_message"]
LAYER_ORDER = {"bronze": 1, "silver": 2, "gold": 3}


def latest_attempts(rows: list[dict]) -> list[dict]:
    """A repaired run can log a table twice. Keep the last attempt per (layer, table)."""
    latest: dict[tuple, dict] = {}
    for r in sorted(rows, key=lambda r: str(r.get("end_time") or "")):
        latest[(r["layer"], r["table_name"])] = r
    return sorted(latest.values(), key=lambda r: (LAYER_ORDER.get(str(r["layer"]).lower(), 9), str(r["table_name"])))


def overall_status(rows: list[dict], expected_tables: int | None = None) -> str:
    if not rows:
        return "FAILED"
    if any(r["status"] != "SUCCESS" for r in rows):
        return "FAILED"
    if expected_tables is not None and len(rows) < expected_tables:
        return "INCOMPLETE"
    return "SUCCESS"


def build_html(rows: list[dict], run_id: str, status: str, bank_name: str) -> str:
    colour = {"SUCCESS": "#1a7f37", "FAILED": "#cf222e"}.get(status, "#9a6700")
    head = "".join(f"<th style='text-align:left;padding:6px 10px;border-bottom:2px solid #ccc'>{c.replace('_', ' ').title()}</th>" for c in COLUMNS)
    body = []
    for r in rows:
        cells = []
        for c in COLUMNS:
            v = r.get(c)
            text = "" if v is None else (f"{v:,}" if isinstance(v, int) else str(v))
            style = "padding:6px 10px;border-bottom:1px solid #eee"
            if c == "status":
                style += f";font-weight:600;color:{'#1a7f37' if v == 'SUCCESS' else '#cf222e'}"
            cells.append(f"<td style='{style}'>{html.escape(text[:300])}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    total_rows = sum(int(r.get("rows_processed") or 0) for r in rows)
    return f"""<html><body style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222">
<p style="font-size:18px">{html.escape(bank_name)} · LedgerLake run
<b style="color:{colour}">{status}</b></p>
<p>Run ID: <code>{html.escape(str(run_id))}</code> · {len(rows)} table loads · {total_rows:,} rows processed</p>
<table style="border-collapse:collapse">{"<tr>" + head + "</tr>"}{"".join(body)}</table>
<p style="color:#666;font-size:12px">Automated message from the LedgerLake pipeline. Source: metadata.pipeline_runs.</p>
</body></html>"""


def subject(status: str, bank_name: str) -> str:
    return f"{bank_name} | LedgerLake | {status}"


def send_gmail(sender: str, app_password: str, recipients: list[str], subject_line: str, html_body: str) -> None:
    """Send through Gmail SMTP with an app password (Google account > Security > App passwords)."""
    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject_line
    msg.attach(MIMEText(html_body, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
        smtp.login(sender, app_password)
        smtp.sendmail(sender, recipients, msg.as_string())
