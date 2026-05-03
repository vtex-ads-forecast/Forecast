#!/usr/bin/env python3
"""
Backfill missing April 2026 days (28-30) into REAL_MONTHLY and MONTHS_DATA.
Does NOT touch current month structures (NA, ACTUALS, REAL_APRIL, etc).

Usage:
  METABASE_USER=you@email METABASE_PASS=secret python3 scripts/backfill_april.py
"""

import os
import sys
import re
import json
import requests
from datetime import datetime
from collections import defaultdict

METABASE_URL = "https://metabase.newtail.com.br"
HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "index.html")
SETTINGS_PATH = os.path.join(os.path.dirname(__file__), "..", "settings.json")

FX_RATES = {"BRL": 1.0, "ARS": 0.0036, "COP": 0.0015, "PEN": 1.50}
EXCLUDE_PATTERNS = ["teste", "test", "staging", "hml", "homolog"]
PROTECTED_SEGMENTS = {"Others"}

START_DATE = "2026-04-28"
END_DATE = "2026-04-30"
MONTH_KEY = "2026-04"


def metabase_auth():
    user = os.environ.get("METABASE_USER")
    pwd = os.environ.get("METABASE_PASS")
    if not user or not pwd:
        raise ValueError("METABASE_USER and METABASE_PASS env vars required")
    resp = requests.post(f"{METABASE_URL}/api/session",
                         json={"username": user, "password": pwd}, timeout=30)
    resp.raise_for_status()
    print(f"✓ Authenticated as {user}")
    return resp.json().get("id")


def fetch_data(token):
    headers = {"X-Metabase-Session": token}
    DB_ID = 13
    PAGE_SIZE = 2000

    base_sql = f"""
    with metrics AS (
        SELECT cmnd.day, cmnd.campaign_id, cmnd.publisher_id, cmnd.advertiser_id,
               SUM(cmnd.total_clicks_cost) + SUM(cmnd.total_impressions_cost) AS total_cost,
               p.name AS publisher_name, p.currency_code, p.is_test
        FROM CAMPAIGNS_METRICS_NETWORK_DAY cmnd
        JOIN publishers p ON p.id = cmnd.publisher_id
        WHERE day BETWEEN ('{START_DATE}')::timestamp - INTERVAL '1 day'
                      AND ('{END_DATE}')::timestamp + INTERVAL '2 days'
        GROUP BY cmnd.day, cmnd.campaign_id, cmnd.publisher_id, cmnd.advertiser_id,
                 p.name, p.currency_code, p.is_test
    ),
    costs AS (
        SELECT r.day, r.publisher_id, r.publisher_name, r.advertiser_id,
               a.name AS advertiser_name, r.currency_code, r.campaign_id,
               SUM(r.total_cost) AS total_cost
        FROM metrics r
        LEFT JOIN advertisers a ON a.id = r.advertiser_id
        WHERE r.day >= '{START_DATE}'
          AND r.day < '{END_DATE}'::date + INTERVAL '1 day'
          AND r.is_test = false
        GROUP BY r.day, r.publisher_id, r.publisher_name, r.advertiser_id,
                 a.name, r.currency_code, r.campaign_id
    )
    SELECT day, publisher_id, publisher_name, advertiser_id, advertiser_name,
           currency_code, campaign_id, total_cost
    FROM costs WHERE total_cost > 0 ORDER BY day DESC
    """

    all_rows = []
    cols = []
    offset = 0
    while True:
        paginated_sql = f"{base_sql} LIMIT {PAGE_SIZE} OFFSET {offset}"
        print(f"  Fetching offset={offset}...")
        resp = requests.post(f"{METABASE_URL}/api/dataset", headers=headers,
                             json={"database": DB_ID, "type": "native",
                                   "native": {"query": paginated_sql}}, timeout=300)
        resp.raise_for_status()
        result = resp.json()
        if not cols:
            cols = [c.get("name", f"col_{i}") for i, c in enumerate(result.get("data", {}).get("cols", []))]
        page_rows = result.get("data", {}).get("rows", [])
        print(f"  Got {len(page_rows)} rows")
        all_rows.extend(page_rows)
        if len(page_rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    col_idx = {}
    for i, c in enumerate(cols):
        cl = c.lower().replace(" ", "_")
        if "day" in cl or "date" in cl: col_idx["day"] = i
        elif "publisher" in cl and "name" in cl: col_idx["publisher_name"] = i
        elif "advertiser" in cl and "name" in cl: col_idx["advertiser_name"] = i
        elif "currency" in cl: col_idx["currency_code"] = i
        elif "cost" in cl or "total" in cl: col_idx["total_cost"] = i

    data = []
    for row in all_rows:
        data.append({
            "day": str(row[col_idx.get("day", 0)] or "")[:10],
            "publisher_name": str(row[col_idx.get("publisher_name", 2)] or ""),
            "advertiser_name": str(row[col_idx.get("advertiser_name", 4)] or ""),
            "currency_code": str(row[col_idx.get("currency_code", 5)] or "BRL"),
            "total_cost": float(row[col_idx.get("total_cost", 7)] or 0),
        })
    print(f"✓ Fetched {len(data)} rows")
    return data


def load_pub_mapping():
    """Load publisher → segment mapping from settings.json and HTML."""
    pub_seg = {}
    pub_tr = {}

    # From settings.json
    if os.path.exists(SETTINGS_PATH):
        with open(SETTINGS_PATH) as f:
            settings = json.load(f)
        for pname, info in settings.items():
            if info.get("seg"):
                pub_seg[pname] = info["seg"]
            pub_tr[pname] = {
                "tech": info.get("trTech", 0.1),
                "net": info.get("trNetwork", 0.15)
            }
        print(f"✓ Loaded {len(settings)} publishers from settings.json")

    # From HTML REAL_APRIL (backup)
    with open(HTML_PATH) as f:
        html = f.read()
    # Extract from REAL_MONTHLY segments (more reliable since REAL_APRIL is zeroed)
    rm_segs = re.findall(r'"(\w[^"]+)":\{', html[html.find("const REAL_MONTHLY"):html.find("const REAL_MONTHLY") + 2000])
    print(f"  REAL_MONTHLY segments: {rm_segs}")

    return pub_seg, pub_tr


def process(raw_data, pub_seg, pub_tr):
    """Compute segment-level spend/rev deltas."""
    seg_delta = defaultdict(lambda: {"sp": 0, "rv": 0})
    daily_total = defaultdict(float)

    for r in raw_data:
        adv = (r.get("advertiser_name") or "").strip()
        if any(x in adv.lower() for x in EXCLUDE_PATTERNS):
            continue
        cost = float(r.get("total_cost") or 0)
        if cost <= 0:
            continue
        pub = (r.get("publisher_name") or "").strip()
        curr = (r.get("currency_code") or "BRL").strip()
        fx = FX_RATES.get(curr, 1.0)
        cost_brl = cost * fx

        day_str = str(r.get("day", ""))[:10]
        day_num = int(day_str.split("-")[2])
        daily_total[day_num] += cost_brl

        seg = pub_seg.get(pub, "Long Tail")
        if seg in PROTECTED_SEGMENTS:
            seg = "Long Tail"

        is_net = "vtexads" in adv.lower()
        tri = pub_tr.get(pub, {"tech": 0.1, "net": 0.15})
        tr = tri["net"] if is_net else tri["tech"]
        rev = cost_brl * tr

        seg_delta[seg]["sp"] += cost_brl
        seg_delta[seg]["rv"] += rev

    total_spend = sum(daily_total.values())
    total_rev = sum(d["rv"] for d in seg_delta.values())
    print(f"\n  Days found: {sorted(daily_total.keys())}")
    print(f"  Total spend (BRL): {total_spend:,.0f}")
    print(f"  Total revenue (BRL): {total_rev:,.0f}")

    for seg, d in sorted(seg_delta.items()):
        print(f"    {seg}: sp={d['sp']:,.0f} rv={d['rv']:,.0f}")

    return seg_delta, daily_total


def apply_to_html(seg_delta, daily_total):
    """Update ONLY REAL_MONTHLY and MONTHS_DATA for April."""
    with open(HTML_PATH) as f:
        html = f.read()

    total_spend_add = sum(daily_total.values())
    total_rev_add = sum(d["rv"] for d in seg_delta.values())

    # 1. Update REAL_MONTHLY 2026-04 entries
    for seg, delta in seg_delta.items():
        if seg in PROTECTED_SEGMENTS:
            continue
        rm_start_search = html.find("const REAL_MONTHLY")
        seg_start = html.find(f'"{seg}":', rm_start_search)
        if seg_start == -1:
            print(f"  ⚠ Segment {seg} not found in REAL_MONTHLY")
            continue

        # Find the segment's object
        depth = 0
        seg_obj_start = html.find("{", seg_start + len(f'"{seg}":'))
        seg_obj_end = seg_obj_start
        for i in range(seg_obj_start, len(html)):
            if html[i] == "{": depth += 1
            elif html[i] == "}":
                depth -= 1
                if depth == 0:
                    seg_obj_end = i
                    break

        seg_block = html[seg_obj_start:seg_obj_end + 1]

        pat = rf'"2026-04":\{{spend:(\d+),rev:(\d+)\}}'
        m = re.search(pat, seg_block)
        if m:
            old_sp = int(m.group(1))
            old_rv = int(m.group(2))
            old_str = f'"2026-04":{{spend:{old_sp},rev:{old_rv}}}'
            new_str = f'"2026-04":{{spend:{old_sp + round(delta["sp"])},rev:{old_rv + round(delta["rv"])}}}'
            new_block = seg_block.replace(old_str, new_str, 1)
            html = html[:seg_obj_start] + new_block + html[seg_obj_end + 1:]
            print(f"  ✓ REAL_MONTHLY[{seg}][2026-04]: spend {old_sp:,}→{old_sp+round(delta['sp']):,}, rev {old_rv:,}→{old_rv+round(delta['rv']):,}")

    # 2. Update MONTHS_DATA 2026-04 realSpend and realRev
    apr_pat = r'("2026-04":\s*\{[^}]*?realSpend:\s*)(\d+)'
    m = re.search(apr_pat, html)
    if m:
        old_sp = int(m.group(2))
        new_sp = old_sp + round(total_spend_add)
        html = html[:m.start(2)] + str(new_sp) + html[m.end(2):]
        print(f"  ✓ MONTHS_DATA[2026-04].realSpend: {old_sp:,} → {new_sp:,}")

    apr_pat2 = r'("2026-04":\s*\{[^}]*?realRev:\s*)(\d+)'
    m2 = re.search(apr_pat2, html)
    if m2:
        old_rv = int(m2.group(2))
        new_rv = old_rv + round(total_rev_add)
        html = html[:m2.start(2)] + str(new_rv) + html[m2.end(2):]
        print(f"  ✓ MONTHS_DATA[2026-04].realRev: {old_rv:,} → {new_rv:,}")

    # 3. Update PREV_MONTH_SPEND and PREV_MONTH_REV
    html = re.sub(
        r'const PREV_MONTH_SPEND = \d+;',
        f'const PREV_MONTH_SPEND = {round(9550607 + total_spend_add)};',
        html
    )
    html = re.sub(
        r'const PREV_MONTH_REV = \d+;',
        f'const PREV_MONTH_REV = {round(1193215 + total_rev_add)};',
        html
    )
    print(f"  ✓ PREV_MONTH updated")

    with open(HTML_PATH, "w") as f:
        f.write(html)
    print(f"\n✓ Saved! April backfill complete.")


def main():
    print(f"\n{'='*50}")
    print(f"  April 2026 Backfill (days 28-30)")
    print(f"{'='*50}\n")

    token = metabase_auth()
    raw_data = fetch_data(token)
    if not raw_data:
        print("⚠ No data. Exiting.")
        return

    pub_seg, pub_tr = load_pub_mapping()
    seg_delta, daily_total = process(raw_data, pub_seg, pub_tr)
    apply_to_html(seg_delta, daily_total)


if __name__ == "__main__":
    main()
