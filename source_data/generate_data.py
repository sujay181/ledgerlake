"""Synthetic source data for Coral Digital Bank (a fictional Australian neobank).

Produces two batches so the pipeline can be tested for a historical load and an
incremental load:

    <out>/sqlserver/batch_01_historical/   branches, customers, accounts, transactions
    <out>/sqlserver/batch_02_incremental/  customers, accounts, transactions (updates + new rows)
    <out>/landing/credit_bureau_reports/   credit_bureau_reports_batch_01.csv, ..._batch_02.csv
    <out>/landing/payment_gateway_events/  payment_gateway_events_batch_01.csv, ..._batch_02.csv

The SQL Server files are loaded into the source database with load_sqlserver.py.
The landing files are uploaded to the Unity Catalog volume, where Auto Loader picks them up.

Deliberate data issues (so the pipeline has something real to handle):
  * batch 02 credit bureau file pulls some customers twice  -> silver must keep the latest
  * batch 02 gateway file repeats some events                -> silver must not double count
  * a few gateway rows have a non-numeric latency            -> lands in _rescued_data

All timestamps are UTC. Every person, email and phone number is invented.
Standard library only, so it runs anywhere: python generate_data.py --out generated
"""
from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

FIRST_NAMES = [
    "Olivia", "Noah", "Charlotte", "Oliver", "Amelia", "Jack", "Isla", "William", "Mia", "Leo",
    "Ava", "Henry", "Grace", "Lucas", "Chloe", "Thomas", "Zoe", "James", "Ruby", "Ethan",
    "Priya", "Arjun", "Mei", "Wei", "Aisha", "Omar", "Sofia", "Luca", "Hannah", "Daniel",
    "Ananya", "Rahul", "Linh", "Minh", "Fatima", "Ali", "Emily", "Samuel", "Georgia", "Max",
]
LAST_NAMES = [
    "Smith", "Jones", "Williams", "Brown", "Wilson", "Taylor", "Nguyen", "Johnson", "Martin", "White",
    "Anderson", "Walker", "Thompson", "Harris", "Lee", "Ryan", "Kelly", "King", "Patel", "Singh",
    "Chen", "Wang", "Tran", "Kumar", "Sharma", "Ali", "Rossi", "Murphy", "O'Brien", "Campbell",
]
# State, population weight, postcode range, cities
STATES = [
    ("NSW", 0.31, (2000, 2999), ["Sydney", "Parramatta", "Newcastle", "Wollongong"]),
    ("VIC", 0.26, (3000, 3999), ["Melbourne", "Geelong", "Ballarat", "Bendigo"]),
    ("QLD", 0.21, (4000, 4999), ["Brisbane", "Gold Coast", "Townsville"]),
    ("WA", 0.11, (6000, 6999), ["Perth", "Fremantle"]),
    ("SA", 0.07, (5000, 5999), ["Adelaide"]),
    ("TAS", 0.02, (7000, 7499), ["Hobart"]),
    ("ACT", 0.015, (2600, 2620), ["Canberra"]),
    ("NT", 0.005, (800, 899), ["Darwin"]),
]
PRODUCTS = {
    # product: (share of accounts, opening balance range, interest rate range)
    "EVERYDAY": (0.40, (50, 8_000), (0.00, 0.01)),
    "SAVINGS": (0.25, (500, 60_000), (0.035, 0.055)),
    "TERM_DEPOSIT": (0.06, (5_000, 150_000), (0.040, 0.050)),
    "CREDIT_CARD": (0.15, (-6_000, 0), (0.1999, 0.2199)),
    "PERSONAL_LOAN": (0.07, (-40_000, -3_000), (0.079, 0.159)),
    "HOME_LOAN": (0.07, (-900_000, -150_000), (0.055, 0.069)),
}
MERCHANT_CATEGORIES = [
    "GROCERIES", "FUEL", "DINING", "TRANSPORT", "UTILITIES", "RETAIL", "TRAVEL",
    "HEALTH", "ENTERTAINMENT", "EDUCATION",
]
GATEWAYS = ["OrbitPay", "KitePay", "NovaSwitch"]  # fictional processors
KYC_STATUSES = ["VERIFIED"] * 18 + ["PENDING", "UNDER_REVIEW"]

SQL_HEADERS = {
    "branches": ["branch_id", "bsb", "branch_name", "city", "state", "region", "opened_date", "updated_at"],
    "customers": [
        "customer_id", "first_name", "last_name", "date_of_birth", "email", "mobile", "state",
        "postcode", "home_branch_id", "kyc_status", "customer_since", "updated_at",
    ],
    "accounts": [
        "account_id", "customer_id", "product_type", "bsb", "account_status", "opened_date",
        "interest_rate", "credit_limit", "current_balance", "updated_at",
    ],
    "transactions": [
        "transaction_id", "account_id", "txn_timestamp", "direction", "channel", "amount",
        "merchant_category", "description", "txn_status",
    ],
}
CREDIT_HEADER = [
    "report_id", "customer_id", "bureau_name", "pulled_at", "credit_score", "risk_band",
    "open_credit_accounts", "defaults_last_5y", "external_debt_aud",
]
GATEWAY_HEADER = [
    "event_id", "transaction_id", "gateway", "event_timestamp", "event_status", "response_code",
    "latency_ms", "fee_aud",
]


@dataclass
class Account:
    account_id: str
    customer_id: str
    product: str
    bsb: str
    opened: date
    rate: float
    credit_limit: float | None
    balance: float
    status: str = "OPEN"
    updated_at: datetime | None = None


@dataclass
class World:
    rng: random.Random
    branches: list[dict] = field(default_factory=list)
    customers: dict[str, dict] = field(default_factory=dict)
    accounts: dict[str, Account] = field(default_factory=dict)
    txn_seq: int = 0
    event_seq: int = 0
    report_seq: int = 0


def ts(d: datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S")


def risk_band(score: int) -> str:
    if score >= 833:
        return "EXCELLENT"
    if score >= 726:
        return "VERY_GOOD"
    if score >= 622:
        return "GOOD"
    if score >= 510:
        return "FAIR"
    return "POOR"


def random_time(rng: random.Random, day: date) -> datetime:
    # Banking activity skews to daytime (UTC+10/11 local is roughly UTC 21:00-12:00)
    hour = rng.choices(range(24), weights=[3, 4, 5, 5, 5, 5, 4, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 3, 3])[0]
    return datetime(day.year, day.month, day.day, hour, rng.randint(0, 59), rng.randint(0, 59))


def build_branches(w: World) -> None:
    n = 0
    for state, weight, _pc, cities in STATES:
        for city in cities:
            for suffix in (["CBD"] if weight < 0.1 else ["CBD", "North"]):
                n += 1
                w.branches.append({
                    "branch_id": f"BR{n:03d}",
                    "bsb": f"990-{n:03d}",
                    "branch_name": f"{city} {suffix}",
                    "city": city,
                    "state": state,
                    "region": "METRO" if city in ("Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide") else "REGIONAL",
                    "opened_date": date(2016 + n % 8, 1 + n % 12, 1).isoformat(),
                    "updated_at": "2025-07-01 00:00:00",
                })


def new_customer(w: World, cid: int, since: date, created_at: datetime) -> dict:
    rng = w.rng
    state, _w, pc, _cities = rng.choices(STATES, weights=[s[1] for s in STATES])[0]
    branches = [b for b in w.branches if b["state"] == state]
    first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
    dob = date(rng.randint(1950, 2006), rng.randint(1, 12), rng.randint(1, 28))
    return {
        "customer_id": f"C{cid:06d}",
        "first_name": first,
        "last_name": last,
        "date_of_birth": dob.isoformat(),
        "email": f"{first.lower()}.{last.lower().replace(chr(39), '')}{cid}@example.com",
        "mobile": f"04{rng.randint(10, 99)} {rng.randint(100, 999)} {rng.randint(100, 999)}",
        "state": state,
        "postcode": f"{rng.randint(*pc):04d}",
        "home_branch_id": rng.choice(branches)["branch_id"],
        "kyc_status": rng.choice(KYC_STATUSES),
        "customer_since": since.isoformat(),
        "updated_at": ts(created_at),
    }


def open_accounts(w: World, customer: dict, opened: date, created_at: datetime) -> list[Account]:
    rng = w.rng
    products = list(PRODUCTS)
    weights = [PRODUCTS[p][0] for p in products]
    chosen = {"EVERYDAY"}
    for _ in range(rng.choices([0, 1, 2, 3], weights=[35, 40, 18, 7])[0]):
        chosen.add(rng.choices(products, weights=weights)[0])
    branch = next(b for b in w.branches if b["branch_id"] == customer["home_branch_id"])
    out = []
    for product in sorted(chosen):
        _share, bal_range, rate_range = PRODUCTS[product]
        acc_id = f"A{len(w.accounts) + 1:07d}"
        limit = float(rng.choice([2_000, 5_000, 10_000, 15_000])) if product == "CREDIT_CARD" else None
        balance = round(rng.uniform(*bal_range), 2)
        if limit is not None:
            balance = max(balance, -limit * 0.9)
        acc = Account(acc_id, customer["customer_id"], product, branch["bsb"], opened,
                      round(rng.uniform(*rate_range), 4), limit, balance, updated_at=created_at)
        w.accounts[acc_id] = acc
        out.append(acc)
    return out


def txn_row(w: World, acc: Account, when: datetime, direction: str, channel: str, amount: float,
            category: str | None, description: str, status: str = "POSTED") -> dict:
    w.txn_seq += 1
    amount = round(amount, 2)
    if status == "POSTED":
        acc.balance = round(acc.balance + (amount if direction == "CREDIT" else -amount), 2)
        acc.updated_at = max(acc.updated_at or when, when)
    return {
        "transaction_id": f"T{w.txn_seq:09d}",
        "account_id": acc.account_id,
        "txn_timestamp": ts(when),
        "direction": direction,
        "channel": channel,
        "amount": f"{amount:.2f}",
        "merchant_category": category or "",
        "description": description,
        "txn_status": status,
    }


def simulate_day(w: World, day: date, gateway_events: list[dict]) -> list[dict]:
    rng = w.rng
    rows: list[dict] = []
    for acc in w.accounts.values():
        if acc.status != "OPEN" or acc.opened > day:
            continue
        p = acc.product
        if p == "EVERYDAY":
            if day.day in (1, 15) and rng.random() < 0.85:
                rows.append(txn_row(w, acc, random_time(rng, day), "CREDIT", "NPP", rng.uniform(1_200, 4_800), None, "Salary"))
            for _ in range(rng.choices([0, 1, 2, 3], weights=[62, 25, 10, 3])[0]):
                channel = rng.choices(["CARD", "NPP", "BPAY", "ATM", "DIRECT_DEBIT", "BRANCH"], weights=[60, 14, 8, 6, 10, 2])[0]
                amount = rng.lognormvariate(3.4, 0.9) if channel == "CARD" else rng.uniform(20, 600)
                category = rng.choice(MERCHANT_CATEGORIES) if channel == "CARD" else None
                status = "POSTED"
                if channel == "CARD":
                    status = rng.choices(["POSTED", "DECLINED", "REVERSED"], weights=[95.5, 3.5, 1.0])[0]
                if status == "POSTED" and acc.balance - amount < -200:
                    status = "DECLINED" if channel == "CARD" else status
                    if channel != "CARD":
                        continue
                row = txn_row(w, acc, random_time(rng, day), "DEBIT", channel, amount, category, f"{channel} purchase" if channel == "CARD" else f"{channel} payment", status)
                rows.append(row)
                if channel == "CARD":
                    gateway_events.append(gateway_event(w, row))
        elif p == "SAVINGS":
            if day.day == 2 and rng.random() < 0.6:
                rows.append(txn_row(w, acc, random_time(rng, day), "CREDIT", "INTERNAL_TRANSFER", rng.uniform(100, 1_500), None, "Transfer from everyday"))
            if day.day == 28:
                interest = max(acc.balance, 0) * acc.rate / 12
                if interest > 0.01:
                    rows.append(txn_row(w, acc, datetime(day.year, day.month, day.day, 14, 0, 0), "CREDIT", "INTERNAL", interest, None, "Interest"))
        elif p == "CREDIT_CARD":
            for _ in range(rng.choices([0, 1, 2], weights=[72, 23, 5])[0]):
                amount = rng.lognormvariate(3.8, 1.0)
                status = rng.choices(["POSTED", "DECLINED", "REVERSED"], weights=[95.5, 3.5, 1.0])[0]
                if status == "POSTED" and acc.credit_limit and acc.balance - amount < -acc.credit_limit:
                    status = "DECLINED"
                row = txn_row(w, acc, random_time(rng, day), "DEBIT", "CARD", amount, rng.choice(MERCHANT_CATEGORIES), "CARD purchase", status)
                rows.append(row)
                gateway_events.append(gateway_event(w, row))
            if day.day == 20 and acc.balance < 0:
                rows.append(txn_row(w, acc, random_time(rng, day), "CREDIT", "BPAY", min(-acc.balance, rng.uniform(200, 3_000)), None, "Card repayment"))
        elif p in ("PERSONAL_LOAN", "HOME_LOAN"):
            if day.day == 25 and acc.balance < 0:
                interest = -acc.balance * acc.rate / 12
                rows.append(txn_row(w, acc, datetime(day.year, day.month, day.day, 13, 0, 0), "DEBIT", "INTERNAL", interest, None, "Loan interest"))
                repayment = interest * rng.uniform(1.3, 2.2) if p == "HOME_LOAN" else interest + rng.uniform(250, 900)
                rows.append(txn_row(w, acc, datetime(day.year, day.month, day.day, 13, 5, 0), "CREDIT", "DIRECT_DEBIT", min(repayment, -acc.balance), None, "Loan repayment"))
    return rows


def gateway_event(w: World, txn: dict) -> dict:
    rng = w.rng
    w.event_seq += 1
    status = {"POSTED": "SETTLED", "DECLINED": "DECLINED", "REVERSED": "REVERSED"}[txn["txn_status"]]
    code = {"SETTLED": "00", "DECLINED": rng.choice(["05", "51", "54", "61"]), "REVERSED": "R1"}[status]
    when = datetime.strptime(txn["txn_timestamp"], "%Y-%m-%d %H:%M:%S") + timedelta(seconds=rng.randint(1, 90))
    gateway = rng.choices(GATEWAYS, weights=[50, 35, 15])[0]
    latency = int(rng.lognormvariate(5.3, 0.45)) + (250 if gateway == "NovaSwitch" else 0)
    fee = round(float(txn["amount"]) * 0.0045 + 0.05, 2) if status == "SETTLED" else 0.0
    return {
        "event_id": f"E{w.event_seq:09d}",
        "transaction_id": txn["transaction_id"],
        "gateway": gateway,
        "event_timestamp": ts(when),
        "event_status": status,
        "response_code": code,
        "latency_ms": str(latency),
        "fee_aud": f"{fee:.2f}",
    }


def credit_report(w: World, customer_id: str, pulled: datetime, prev_score: int | None = None) -> dict:
    rng = w.rng
    w.report_seq += 1
    score = prev_score + rng.randint(-60, 40) if prev_score else int(rng.gauss(700, 140))
    score = max(150, min(1150, score))
    defaults = 0 if score > 600 else rng.choices([0, 1, 2, 3], weights=[50, 30, 15, 5])[0]
    return {
        "report_id": f"R{w.report_seq:08d}",
        "customer_id": customer_id,
        "bureau_name": rng.choice(["BureauOne", "CreditView"]),  # fictional bureaus
        "pulled_at": ts(pulled),
        "credit_score": str(score),
        "risk_band": risk_band(score),
        "open_credit_accounts": str(rng.randint(0, 7)),
        "defaults_last_5y": str(defaults),
        "external_debt_aud": f"{max(0, rng.gauss(18_000, 22_000)):.2f}",
    }


def account_row(a: Account) -> dict:
    return {
        "account_id": a.account_id,
        "customer_id": a.customer_id,
        "product_type": a.product,
        "bsb": a.bsb,
        "account_status": a.status,
        "opened_date": a.opened.isoformat(),
        "interest_rate": f"{a.rate:.4f}",
        "credit_limit": "" if a.credit_limit is None else f"{a.credit_limit:.2f}",
        "current_balance": f"{a.balance:.2f}",
        "updated_at": ts(a.updated_at),
    }


def write_csv(path: Path, header: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=header)
        wr.writeheader()
        wr.writerows(rows)


def generate(out: Path, seed: int, n_customers: int, history_start: date, history_days: int,
             incremental_days: int) -> dict[str, int]:
    w = World(random.Random(seed))
    rng = w.rng
    build_branches(w)
    history_end = history_start + timedelta(days=history_days)  # exclusive

    # ---- Batch 01: customers who joined over the last three years, then 12 months of activity
    for i in range(1, n_customers + 1):
        since = history_start - timedelta(days=rng.randint(0, 3 * 365))
        created = datetime(since.year, since.month, since.day, 1, 0, 0)
        c = new_customer(w, i, since, created)
        w.customers[c["customer_id"]] = c
        open_accounts(w, c, since, created)

    txns_1, events_1 = [], []
    for d in range(history_days):
        txns_1 += simulate_day(w, history_start + timedelta(days=d), events_1)

    # A few customers changed details during the year
    for c in rng.sample(list(w.customers.values()), k=max(1, n_customers // 20)):
        c["kyc_status"] = "VERIFIED"
        c["updated_at"] = ts(random_time(rng, history_start + timedelta(days=rng.randint(0, history_days - 1))))

    credit_1 = []
    last_score: dict[str, int] = {}
    for cid in w.customers:
        pulled = random_time(rng, history_start + timedelta(days=rng.randint(0, history_days - 1)))
        rep = credit_report(w, cid, pulled)
        last_score[cid] = int(rep["credit_score"])
        credit_1.append(rep)

    # Three malformed latency values: Auto Loader keeps the row and parks the bad value in _rescued_data
    for e in rng.sample(events_1, k=min(3, len(events_1))):
        e["latency_ms"] = "timeout"

    b1 = out / "sqlserver" / "batch_01_historical"
    write_csv(b1 / "branches.csv", SQL_HEADERS["branches"], w.branches)
    write_csv(b1 / "customers.csv", SQL_HEADERS["customers"], list(w.customers.values()))
    write_csv(b1 / "accounts.csv", SQL_HEADERS["accounts"], [account_row(a) for a in w.accounts.values()])
    write_csv(b1 / "transactions.csv", SQL_HEADERS["transactions"], txns_1)
    land = out / "landing"
    write_csv(land / "credit_bureau_reports" / "credit_bureau_reports_batch_01.csv", CREDIT_HEADER, credit_1)
    write_csv(land / "payment_gateway_events" / "payment_gateway_events_batch_01.csv", GATEWAY_HEADER, events_1)

    # ---- Batch 02: one more week. Updates to existing customers, new customers, new activity.
    snapshot = {aid: (a.balance, a.updated_at) for aid, a in w.accounts.items()}
    changed_customers: dict[str, dict] = {}
    for c in rng.sample(list(w.customers.values()), k=max(1, n_customers * 3 // 100)):
        c["state"], _w, pc, _cities = rng.choices(STATES, weights=[s[1] for s in STATES])[0]
        c["postcode"] = f"{rng.randint(*pc):04d}"
        c["home_branch_id"] = rng.choice([b for b in w.branches if b["state"] == c["state"]])["branch_id"]
        c["updated_at"] = ts(random_time(rng, history_end + timedelta(days=rng.randint(0, incremental_days - 1))))
        changed_customers[c["customer_id"]] = c
    new_accounts: list[Account] = []
    for i in range(n_customers + 1, n_customers + 1 + max(1, n_customers // 50)):
        since = history_end + timedelta(days=rng.randint(0, incremental_days - 1))
        created = random_time(rng, since)
        c = new_customer(w, i, since, created)
        w.customers[c["customer_id"]] = c
        changed_customers[c["customer_id"]] = c
        new_accounts += open_accounts(w, c, since, created)

    txns_2, events_2 = [], []
    for d in range(incremental_days):
        txns_2 += simulate_day(w, history_end + timedelta(days=d), events_2)

    changed_accounts = [a for aid, a in w.accounts.items()
                        if aid not in snapshot or snapshot[aid] != (a.balance, a.updated_at)]

    credit_2 = []
    for cid in rng.sample(list(last_score), k=max(1, n_customers * 8 // 100)):
        pulled = random_time(rng, history_end + timedelta(days=rng.randint(0, incremental_days - 1)))
        credit_2.append(credit_report(w, cid, pulled, last_score[cid]))
    # Some customers pulled twice in the same week: silver must keep only the latest pull
    for rep in rng.sample(credit_2, k=min(10, len(credit_2))):
        later = datetime.strptime(rep["pulled_at"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=rng.randint(1, 30))
        credit_2.append(credit_report(w, rep["customer_id"], later, int(rep["credit_score"])))
    # Some gateway events delivered twice: silver must not double count them
    events_2 += [dict(e) for e in rng.sample(events_2, k=min(15, len(events_2)))]

    b2 = out / "sqlserver" / "batch_02_incremental"
    write_csv(b2 / "customers.csv", SQL_HEADERS["customers"], list(changed_customers.values()))
    write_csv(b2 / "accounts.csv", SQL_HEADERS["accounts"], [account_row(a) for a in changed_accounts])
    write_csv(b2 / "transactions.csv", SQL_HEADERS["transactions"], txns_2)
    write_csv(land / "credit_bureau_reports" / "credit_bureau_reports_batch_02.csv", CREDIT_HEADER, credit_2)
    write_csv(land / "payment_gateway_events" / "payment_gateway_events_batch_02.csv", GATEWAY_HEADER, events_2)

    return {
        "branches": len(w.branches),
        "customers_batch_01": n_customers,
        "accounts_batch_01": len(snapshot),
        "transactions_batch_01": len(txns_1),
        "credit_reports_batch_01": len(credit_1),
        "gateway_events_batch_01": len(events_1),
        "customers_batch_02": len(changed_customers),
        "accounts_batch_02": len(changed_accounts),
        "transactions_batch_02": len(txns_2),
        "credit_reports_batch_02": len(credit_2),
        "gateway_events_batch_02": len(events_2),
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=Path("generated"))
    p.add_argument("--seed", type=int, default=20261023)
    p.add_argument("--customers", type=int, default=1500)
    p.add_argument("--history-start", type=date.fromisoformat, default=date(2025, 10, 1))
    p.add_argument("--history-days", type=int, default=365)
    p.add_argument("--incremental-days", type=int, default=7)
    a = p.parse_args()
    counts = generate(a.out, a.seed, a.customers, a.history_start, a.history_days, a.incremental_days)
    for k, v in counts.items():
        print(f"{k:28s} {v:>10,}")


if __name__ == "__main__":
    main()
