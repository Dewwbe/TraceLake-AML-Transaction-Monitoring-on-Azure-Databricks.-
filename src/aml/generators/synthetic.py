"""Deterministic synthetic data generators (SYNTHETIC, not real people or transactions).

* ``generate_transactions``: SAML-D-like transaction file with injected typologies and
  deliberately dirty rows, so tests and demos run without downloading any dataset.
* ``generate_customers``: customer master and account mapping derived from account ids.
* ``generate_fictional_sanctions``: a FICTIONAL sanctions list. Names are random combinations;
  any resemblance to real persons is coincidental.
* ``seed_name_matches``: plants perturbed versions of listed names into some customers and
  records the ground truth, so screening recall can be measured.

Everything is seeded and reproducible.
"""

from __future__ import annotations

import math
import random
from datetime import date, datetime, timedelta

import pandas as pd

SAML_COLUMNS = [
    "Time", "Date", "Sender_account", "Receiver_account", "Amount", "Payment_currency",
    "Received_currency", "Sender_bank_location", "Receiver_bank_location", "Payment_type",
    "Is_laundering", "Laundering_type",
]

COUNTRIES = ["UAE", "UK", "Pakistan", "Morocco", "Turkey", "Nigeria", "Mexico", "India", "Switzerland", "Austria"]
HIGH_RISK_DEMO = ["Nigeria", "Mexico"]  # demo only; see config/country_risk_template.csv
PAYMENT_TYPES = ["Credit card", "Debit card", "Cheque", "ACH", "Cross-border", "Cash Deposit", "Cash Withdrawal"]
CURRENCY_LABELS = {"UAE": "Dirham", "UK": "UK pounds", "Pakistan": "Pakistani rupee", "Morocco": "Moroccan dirham",
                   "Turkey": "Turkish lira", "Nigeria": "Naira", "Mexico": "Mexican Peso", "India": "Indian rupee",
                   "Switzerland": "Swiss franc", "Austria": "Euro"}
# Rough AED per unit, used only to size synthetic amounts. Not a rate source.
APPROX_AED = {"Dirham": 1.0, "UK pounds": 4.7, "Pakistani rupee": 0.013, "Moroccan dirham": 0.37,
              "Turkish lira": 0.1, "Naira": 0.0025, "Mexican Peso": 0.2, "Indian rupee": 0.044,
              "Swiss franc": 4.2, "Euro": 4.0, "US dollar": 3.6725}

FIRST = ["Mohammed", "Ahmed", "Hussein", "Yusuf", "Omar", "Ali", "Khalid", "Sara", "Layla", "Fatima",
         "Viktor", "Dmitri", "Elena", "Nikolai", "Anna", "Rahul", "Priya", "Arjun", "Kavya", "Daniel",
         "Sofia", "Lucas", "Maria", "Jorge", "Chinedu", "Amara", "Emre", "Zeynep", "Hans", "Marta"]
LAST = ["Al Mansoori", "Haddad", "Rahimi", "Qureshi", "Bennani", "Yilmaz", "Okafor", "Fernandez", "Sharma",
        "Patel", "Volkov", "Petrov", "Novak", "Keller", "Meier", "Costa", "Silva", "Ibrahim", "Nasser", "Farouk",
        "Hamdan", "Suleiman", "Ortega", "Demir", "Adeyemi", "Kapoor", "Reyes", "Orlenko", "Lindqvist", "Brandt"]
OCCUPATIONS = ["Engineer", "Teacher", "Trader", "Accountant", "Driver", "Consultant", "Shop owner", "Nurse",
               "Developer", "Manager", "Student", "Retired"]
TRANSLIT = {"mohammed": "muhammad", "ahmed": "ahmad", "hussein": "hussain", "yusuf": "yousef",
            "omar": "umar", "ali": "aly", "khalid": "khaled", "viktor": "victor", "dmitri": "dmitry"}


def _rand_ts(rng: random.Random, start: datetime, days: int) -> datetime:
    return start + timedelta(days=rng.randrange(days), seconds=rng.randrange(86400))


def generate_transactions(
    n_accounts: int = 300, n_normal: int = 6000, days: int = 30, seed: int = 7,
    start: datetime = datetime(2024, 1, 1),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (transactions in SAML-D layout, account table with home country)."""
    rng = random.Random(seed)
    accounts = [f"{10_000_000 + i}" for i in range(n_accounts)]
    home = {a: rng.choices(COUNTRIES, weights=[5, 2, 1, 1, 1, 1, 1, 2, 1, 1])[0] for a in accounts}
    rows: list[list] = []

    def add(ts, s, r, amount_aed, ccy_label, ptype, label=0, typ="Normal", s_country=None, r_country=None):
        amount = round(amount_aed / APPROX_AED[ccy_label], 2)
        rows.append([ts.strftime("%H:%M:%S"), ts.strftime("%Y-%m-%d"), s, r, amount, ccy_label, ccy_label,
                     s_country or home[s], r_country or home[r], ptype, label, typ])

    for _ in range(n_normal):
        s, r = rng.sample(accounts, 2)
        label = CURRENCY_LABELS[home[s]]
        amt = min(math.exp(rng.gauss(7.0, 1.0)), 30_000)
        add(_rand_ts(rng, start, days), s, r, amt, label, rng.choice(PAYMENT_TYPES))

    pool = list(accounts)
    rng.shuffle(pool)
    it = iter(pool)

    for _ in range(4):  # structuring
        a = next(it)
        t0 = _rand_ts(rng, start, days - 3)
        for k in range(rng.randint(4, 6)):
            add(t0 + timedelta(hours=7 * k), a, rng.choice(accounts), rng.uniform(41_000, 49_500),
                "Dirham", "Cash Deposit", 1, "Structuring", s_country="UAE")
    for _ in range(4):  # rapid pass-through
        a, src, dst = next(it), next(it), next(it)
        t0 = _rand_ts(rng, start, days - 2)
        amt = rng.uniform(90_000, 200_000)
        add(t0, src, a, amt, "Dirham", "ACH", 1, "Pass_through", s_country="UAE", r_country="UAE")
        add(t0 + timedelta(hours=3), a, dst, amt * 0.95, "Dirham", "ACH", 1, "Pass_through",
            s_country="UAE", r_country="UAE")
    for _ in range(3):  # fan-out
        a = next(it)
        t0 = _rand_ts(rng, start, days - 1)
        for k, r in enumerate(rng.sample(accounts, 12)):
            add(t0 + timedelta(minutes=20 * k), a, r, rng.uniform(8_000, 11_000), "Dirham", "ACH", 1, "Fan_out",
                s_country="UAE")
    for _ in range(3):  # high-risk jurisdiction (cross-border)
        a, r = next(it), next(it)
        t0 = _rand_ts(rng, start, days)
        add(t0, a, r, rng.uniform(30_000, 90_000), "Dirham", "Cross-border", 1, "High_risk_countries",
            s_country="UAE", r_country=rng.choice(HIGH_RISK_DEMO))
    for _ in range(3):  # round amounts
        a = next(it)
        t0 = _rand_ts(rng, start, days - 5)
        for k in range(4):
            add(t0 + timedelta(days=k), a, rng.choice(accounts), rng.choice([10_000, 20_000, 25_000]),
                "Dirham", "Cheque", 1, "Round_amounts", s_country="UAE")

    df = pd.DataFrame(rows, columns=SAML_COLUMNS)
    df = df.sort_values(["Date", "Time"]).reset_index(drop=True)

    # Deliberately dirty rows (labelled normal) to exercise data quality rules.
    bad = []
    base = df.iloc[0].to_dict()
    bad.append({**base, "Amount": ""})                        # null amount
    bad.append({**base, "Amount": "-500.00"})                 # negative amount
    bad.append({**base, "Payment_currency": "Unobtainium", "Received_currency": "Unobtainium"})  # unknown currency
    bad.append({**base, "Date": "not-a-date"})                # invalid date
    bad.append({**base, "Sender_account": ""})                # missing sender
    bad.append(dict(df.iloc[5].to_dict()))                    # exact duplicate row
    df = pd.concat([df, pd.DataFrame(bad)], ignore_index=True)
    df = df.sort_values(["Date", "Time"], kind="stable").reset_index(drop=True)

    acct = pd.DataFrame({"account_id": accounts, "home_country": [home[a] for a in accounts]})
    return df, acct


def generate_customers(account_ids: list[str], seed: int = 11) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (customers, accounts) with 1-3 accounts per customer."""
    rng = random.Random(seed)
    accounts = list(account_ids)
    rng.shuffle(accounts)
    customers, mapping = [], []
    i = 0
    cid = 0
    while i < len(accounts):
        cid += 1
        n = min(rng.choice([1, 1, 1, 2, 2, 3]), len(accounts) - i)
        customer_id = f"C{cid:06d}"
        onboard = date(2018, 1, 1) + timedelta(days=rng.randrange(0, 2000))
        dob = date(1950, 1, 1) + timedelta(days=rng.randrange(0, 365 * 50))
        nat = rng.choices(COUNTRIES, weights=[5, 2, 1, 1, 1, 1, 1, 2, 1, 1])[0]
        customers.append({
            "customer_id": customer_id,
            "full_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
            "date_of_birth": dob.isoformat(),
            "nationality": nat.upper(),
            "residence_country": rng.choices(COUNTRIES, weights=[8, 1, 1, 1, 1, 1, 1, 1, 1, 1])[0].upper(),
            "occupation": rng.choice(OCCUPATIONS),
            "onboarding_date": onboard.isoformat(),
        })
        for a in accounts[i:i + n]:
            mapping.append({"account_id": a, "customer_id": customer_id,
                            "account_open_date": (onboard + timedelta(days=rng.randrange(0, 60))).isoformat()})
        i += n
    return pd.DataFrame(customers), pd.DataFrame(mapping)


def generate_fictional_sanctions(n: int = 60, seed: int = 21) -> pd.DataFrame:
    """FICTIONAL list in canonical layout: ent_num, record_type, alt_num, name, entity_type, program, remarks."""
    rng = random.Random(seed)
    rows, used = [], set()
    ent = 1000
    while len([r for r in rows if r["record_type"] == "primary"]) < n:
        name = f"{rng.choice(FIRST)} {rng.choice(LAST)}"
        if name in used:
            continue
        used.add(name)
        ent += 1
        rows.append({"ent_num": ent, "record_type": "primary", "alt_num": "", "name": name,
                     "entity_type": "individual", "program": "FICTIONAL-PROGRAM", "remarks": "fictional"})
        if rng.random() < 0.25:
            rows.append({"ent_num": ent, "record_type": "alias", "alt_num": 1,
                         "name": f"{name.split()[-1]}, {name.split()[0]}", "entity_type": "",
                         "program": "", "remarks": "fictional alias"})
    for _ in range(5):
        ent += 1
        rows.append({"ent_num": ent, "record_type": "primary", "alt_num": "",
                     "name": f"{rng.choice(LAST)} {rng.choice(['Trading', 'Holdings', 'Logistics'])} FZE",
                     "entity_type": "entity", "program": "FICTIONAL-PROGRAM", "remarks": "fictional"})
    return pd.DataFrame(rows)


def perturb_name(name: str, kind: str, rng: random.Random) -> str:
    """Return a perturbed version of ``name`` (kind: exact, reorder, typo, translit, initial, middle)."""
    tokens = name.split()
    if kind == "exact" or len(tokens) < 2:
        return name
    if kind == "reorder":
        return f"{tokens[-1]}, {' '.join(tokens[:-1])}"
    if kind == "initial":
        return f"{tokens[0][0]}. {' '.join(tokens[1:])}"
    if kind == "middle":
        return " ".join([tokens[0], rng.choice(["Ben", "Jan", "Lee"]), *tokens[1:]])
    if kind == "translit":
        low = tokens[0].lower()
        if low in TRANSLIT:
            return " ".join([TRANSLIT[low].capitalize(), *tokens[1:]])
        kind = "typo"
    if kind == "typo":
        idx = max(range(len(tokens)), key=lambda i: len(tokens[i]))
        t = tokens[idx]
        if len(t) >= 5:
            j = rng.randrange(1, len(t) - 2)
            t = t[:j] + t[j + 1] + t[j] + t[j + 2:]  # swap two adjacent letters
        tokens[idx] = t
        return " ".join(tokens)
    raise ValueError(kind)


def seed_name_matches(
    customers: pd.DataFrame, listed_names: list[str], per_kind: int = 3, seed: int = 31,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Replace some customer names with perturbed listed names. Returns (customers, ground_truth)."""
    rng = random.Random(seed)
    out = customers.copy()
    kinds = ["exact", "reorder", "typo", "translit", "initial", "middle"]
    candidates = list(out.index)
    rng.shuffle(candidates)
    truth = []
    for kind in kinds:
        for _ in range(per_kind):
            if not candidates:
                break
            idx = candidates.pop()
            listed = rng.choice(listed_names)
            out.loc[idx, "full_name"] = perturb_name(listed, kind, rng)
            truth.append({"customer_id": out.loc[idx, "customer_id"], "listed_name": listed, "perturbation": kind})
    return out, pd.DataFrame(truth)
