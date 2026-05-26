#!/usr/bin/env python3
"""
VTEX Ads — Daily AdNetwork advertiser spend for May 2026.
Pulls daily × advertiser data from Metabase, filters AdNetwork only,
and saves to adv_network_daily.json.

Usage:
  python scripts/daily_adv_network.py

Environment variables:
  METABASE_USER  — Metabase login email
  METABASE_PASS  — Metabase login password
"""

import os, json, requests
from collections import defaultdict

METABASE_URL = "https://metabase.newtail.com.br"
DB_ID = 13
FX_RATES = {"BRL": 1.0, "ARS": 0.0036, "COP": 0.0015, "PEN": 1.50}
EXCLUDE_PATTERNS = ["teste", "test", "staging", "hml", "homolog"]
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "adv_network_daily.json")


def metabase_auth():
    user = os.environ.get("METABASE_USER")
    pwd = os.environ.get("METABASE_PASS")
    if not user or not pwd:
        raise ValueError("METABASE_USER and METABASE_PASS required")
    resp = requests.post(f"{METABASE_URL}/api/session",
                         json={"username": user, "password": pwd}, timeout=30)
    resp.raise_for_status()
    print(f"✓ Authenticated as {user}")
    return resp.json()["id"]


def fetch_adnetwork_daily(token, start="2026-05-01", end="2026-05-31"):
    headers = {"X-Metabase-Session": token}
    PAGE_SIZE = 2000

    sql = f"""
    WITH metrics AS (
        SELECT cmnd.day, cmnd.advertiser_id, cmnd.publisher_id,
               SUM(cmnd.total_clicks_cost) + SUM(cmnd.total_impressions_cost) AS total_cost,
               p.name AS publisher_name, p.currency_code, p.is_test
        FROM CAMPAIGNS_METRICS_NETWORK_DAY cmnd
        JOIN publishers p ON p.id = cmnd.publisher_id
        WHERE day >= '{start}' AND day < '{end}'::date + INTERVAL '1 day'
        GROUP BY cmnd.day, cmnd.advertiser_id, cmnd.publisher_id,
                 p.name, p.currency_code, p.is_test
    ),
    costs AS (
        SELECT m.day, m.publisher_name, a.name AS advertiser_name,
               m.currency_code, SUM(m.total_cost) AS total_cost
        FROM metrics m
        LEFT JOIN advertisers a ON a.id = m.advertiser_id
        WHERE m.is_test = false
        GROUP BY m.day, m.publisher_name, a.name, m.currency_code
    )
    SELECT day, publisher_name, advertiser_name, currency_code, total_cost
    FROM costs
    WHERE total_cost > 0
      AND LOWER(advertiser_name) LIKE '%vtexads%'
    ORDER BY day, advertiser_name
    """

    all_rows, cols = [], []
    offset = 0
    while True:
        paged = f"{sql} LIMIT {PAGE_SIZE} OFFSET {offset}"
        print(f"  Fetching offset={offset}...")
        resp = requests.post(f"{METABASE_URL}/api/dataset", headers=headers,
                             json={"database": DB_ID, "type": "native",
                                   "native": {"query": paged}}, timeout=300)
        resp.raise_for_status()
        result = resp.json()
        if not cols:
            cols = [c.get("name", f"col_{i}") for i, c in enumerate(result["data"]["cols"])]
        page = result["data"]["rows"]
        print(f"  Got {len(page)} rows")
        all_rows.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    print(f"✓ Total: {len(all_rows)} rows")
    return cols, all_rows


def process(cols, rows):
    # Build: { advertiser: { day: spend_brl } }
    col_idx = {}
    for i, c in enumerate(cols):
        cl = c.lower().replace(" ", "_")
        if "day" in cl: col_idx["day"] = i
        elif "advertiser" in cl: col_idx["adv"] = i
        elif "currency" in cl: col_idx["curr"] = i
        elif "cost" in cl or "total" in cl: col_idx["cost"] = i

    adv_daily = defaultdict(lambda: defaultdict(float))
    for row in rows:
        adv = str(row[col_idx["adv"]] or "").strip()
        if any(x in adv.lower() for x in EXCLUDE_PATTERNS):
            continue
        day = str(row[col_idx["day"]] or "")[:10]  # YYYY-MM-DD
        curr = str(row[col_idx.get("curr", 3)] or "BRL")
        cost = float(row[col_idx["cost"]] or 0)
        fx = FX_RATES.get(curr, 1.0)
        adv_daily[adv][day] += cost * fx

    # Clean name and build output
    result = {}
    for adv, days in adv_daily.items():
        clean = adv.replace("VTEXADS", "").replace("- VTEXADS", "").strip()
        clean = clean.rstrip(" -")
        daily_sorted = dict(sorted(days.items()))
        total = sum(days.values())
        result[clean] = {
            "raw_name": adv,
            "total": round(total),
            "daily": {d: round(v) for d, v in daily_sorted.items()}
        }

    # Sort by total descending
    result = dict(sorted(result.items(), key=lambda x: -x[1]["total"]))
    return result


def main():
    token = metabase_auth()
    cols, rows = fetch_adnetwork_daily(token)
    data = process(cols, rows)

    with open(OUTPUT, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Saved {len(data)} advertisers to {OUTPUT}")
    print(f"\nTop 10:")
    for i, (name, info) in enumerate(list(data.items())[:10]):
        days = info["daily"]
        peak = max(days.values()) if days else 0
        peak_day = max(days, key=days.get) if days else "?"
        last_day = sorted(days.keys())[-1] if days else "?"
        last_val = days.get(last_day, 0)
        print(f"  {i+1}. {name}: total R${info['total']:,} | "
              f"last={last_day} R${last_val:,} | peak={peak_day} R${peak:,}")


if __name__ == "__main__":
    main()
