#!/usr/bin/env python3
"""
VTEX Ads Forecast — Daily Data Updater
Pulls D-1 data from Metabase, processes it, and updates the dashboard HTML.

Usage:
  python scripts/update.py                    # updates with yesterday's data
  python scripts/update.py 2026-04-20 2026-04-22  # updates with specific date range

Environment variables:
  METABASE_USER  — Metabase login email
  METABASE_PASS  — Metabase login password
"""

import os
import sys
import re
import json
import math
import requests
from datetime import datetime, timedelta
from collections import defaultdict

# ─── CONFIG ───────────────────────────────────────────────────────────
METABASE_URL = "https://metabase.newtail.com.br"
CARD_ID = 2368
HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "index.html")

# Exchange rates (foreign currency → BRL)
FX_RATES = {
    "BRL": 1.0,
    "ARS": 0.0036,
    "COP": 0.0015,
    "PEN": 1.50,
}

# Test/staging patterns to exclude
EXCLUDE_PATTERNS = ["teste", "test", "staging", "hml", "homolog"]


# ─── METABASE AUTH ────────────────────────────────────────────────────
def metabase_auth():
    """Authenticate with Metabase and return session token."""
    user = os.environ.get("METABASE_USER")
    pwd = os.environ.get("METABASE_PASS")
    if not user or not pwd:
        raise ValueError("METABASE_USER and METABASE_PASS env vars are required")

    resp = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": user, "password": pwd},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("id")
    print(f"✓ Authenticated as {user}")
    return token


# ─── FETCH DATA ───────────────────────────────────────────────────────
def fetch_data(token, start_date, end_date):
    """Fetch data from Metabase card #2368 with date parameters."""
    headers = {"X-Metabase-Session": token}

    # Get card info to extract template-tag IDs and database_id
    card_resp = requests.get(
        f"{METABASE_URL}/api/card/{CARD_ID}",
        headers=headers,
        timeout=30,
    )
    card_resp.raise_for_status()
    card_info = card_resp.json()

    # Extract template tags to build correct parameter targets
    dataset_query = card_info.get("dataset_query", {})
    template_tags = dataset_query.get("native", {}).get("template-tags", {})
    print(f"  Template tags found: {list(template_tags.keys())}")

    # Build parameters list matching the card's template tags
    parameters = []
    for tag_name, tag_info in template_tags.items():
        tag_id = tag_info.get("id", "")
        tag_type = tag_info.get("type", "text")
        if "start" in tag_name.lower():
            parameters.append({
                "type": "date/single",
                "value": start_date,
                "target": ["variable", ["template-tag", tag_name]],
                "id": tag_id,
            })
        elif "end" in tag_name.lower():
            parameters.append({
                "type": "date/single",
                "value": end_date,
                "target": ["variable", ["template-tag", tag_name]],
                "id": tag_id,
            })

    print(f"  Parameters: {parameters}")

    # Use /api/card/{id}/query with proper parameters + no row limit
    resp = requests.post(
        f"{METABASE_URL}/api/card/{CARD_ID}/query",
        headers=headers,
        json={
            "parameters": parameters,
            "constraints": {"max-results": 100000, "max-results-bare-rows": 100000},
        },
        timeout=300,
    )
    resp.raise_for_status()
    result = resp.json()

    # Parse response: {data: {cols: [...], rows: [...]}}
    cols = [c.get("name", f"col_{i}") for i, c in enumerate(result.get("data", {}).get("cols", []))]
    rows_raw = result.get("data", {}).get("rows", [])
    print(f"  Columns: {cols}")
    print(f"  Raw rows: {len(rows_raw)}")

    if not rows_raw:
        print("⚠ No rows returned from Metabase")
        return []

    # Build column index mapping
    col_idx = {}
    for i, c in enumerate(cols):
        cl = c.lower().replace(" ", "_")
        if "day" in cl or "date" in cl:
            col_idx["day"] = i
        elif "publisher" in cl and "name" in cl:
            col_idx["publisher_name"] = i
        elif "advertiser" in cl and "name" in cl:
            col_idx["advertiser_name"] = i
        elif "currency" in cl:
            col_idx["currency_code"] = i
        elif "cost" in cl or "total" in cl:
            col_idx["total_cost"] = i

    print(f"  Column index mapping: {col_idx}")
    print(f"  Sample row: {rows_raw[0]}")

    # Convert to list of dicts
    data = []
    for row in rows_raw:
        data.append({
            "day": str(row[col_idx.get("day", 0)] or "")[:10],
            "publisher_name": str(row[col_idx.get("publisher_name", 2)] or ""),
            "advertiser_name": str(row[col_idx.get("advertiser_name", 4)] or ""),
            "currency_code": str(row[col_idx.get("currency_code", 5)] or "BRL"),
            "total_cost": float(row[col_idx.get("total_cost", 7)] or 0),
        })

    print(f"✓ Fetched {len(data)} rows from Metabase ({start_date} to {end_date})")
    return data


# ─── PARSE HTML ───────────────────────────────────────────────────────
def load_html():
    """Load the dashboard HTML and extract current state."""
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        return f.read()


def extract_pub_mapping(html):
    """Extract publisher → segment and publisher → take rate from REAL_APRIL."""
    pub_seg = {}
    pub_tr = {}

    start = html.find("\nconst REAL_APRIL={")
    if start == -1:
        raise ValueError("REAL_APRIL not found in HTML")
    start += 1

    depth = 0
    end = start
    found_first = False
    for i in range(start, len(html)):
        if html[i] == "{":
            depth += 1
            found_first = True
        elif html[i] == "}":
            depth -= 1
            if found_first and depth == 0:
                end = i + 1
                break

    ra_block = html[start:end]

    seg_names = re.findall(r'\n"([^"]+)":\{spendReal:', ra_block)
    for seg in seg_names:
        seg_start = ra_block.find(f'\n"{seg}":{{spendReal:')
        if seg_start == -1:
            continue
        pub_marker = "publishers:{"
        pub_idx = ra_block.find(pub_marker, seg_start)
        if pub_idx == -1:
            continue
        inner_start = pub_idx + len(pub_marker)
        depth = 1
        inner_end = inner_start
        for i in range(inner_start, len(ra_block)):
            if ra_block[i] == "{":
                depth += 1
            elif ra_block[i] == "}":
                depth -= 1
                if depth == 0:
                    inner_end = i
                    break
        pub_block = ra_block[inner_start:inner_end]
        for pm in re.finditer(
            r'"([^"]+)":\{spendReal:\d+.*?trTech:([\d.]+),trNetwork:([\d.]+)\}',
            pub_block,
        ):
            pname = pm.group(1)
            pub_seg[pname] = seg
            pub_tr[pname] = {"tech": float(pm.group(2)), "net": float(pm.group(3))}

    print(f"✓ Extracted {len(pub_seg)} publisher mappings")
    return pub_seg, pub_tr


def get_current_na(html):
    """Get current NA (actual days) from HTML."""
    m = re.search(r"const NA\s*=\s*(\d+)", html)
    return int(m.group(1)) if m else 0


# ─── PROCESS DATA ────────────────────────────────────────────────────
def process_rows(raw_data, pub_seg, pub_tr):
    """
    Process raw Metabase rows into structured updates.
    Returns: { daily_brl, seg_delta, pub_delta, adv_delta, daily_seg }
    """
    rows = []
    for r in raw_data:
        try:
            adv = (r.get("advertiser_name") or "").strip()
            low = adv.lower()
            if any(x in low for x in EXCLUDE_PATTERNS):
                continue
            cost = float(r.get("total_cost") or 0)
            if cost <= 0:
                continue
            day_str = str(r.get("day", ""))[:10]
            day = datetime.strptime(day_str, "%Y-%m-%d")
            pub = (r.get("publisher_name") or "").strip()
            curr = (r.get("currency_code") or "BRL").strip()
            rows.append({"day": day, "pub": pub, "adv": adv, "currency": curr, "cost": cost})
        except Exception as e:
            print(f"  ⚠ Skipping row: {e} — row: {r}")
            continue

    print(f"✓ Processed {len(rows)} valid rows")

    # Daily total spend (BRL)
    daily_brl = defaultdict(float)
    for r in rows:
        fx = FX_RATES.get(r["currency"], 1.0)
        daily_brl[r["day"].day] += r["cost"] * fx

    # Segment + publisher deltas (BRL)
    seg_delta = defaultdict(lambda: {"sp": 0, "rv": 0, "spT": 0, "spN": 0, "rvT": 0, "rvN": 0})
    pub_delta = defaultdict(lambda: defaultdict(lambda: {"sp": 0, "rv": 0, "spT": 0, "spN": 0, "rvT": 0, "rvN": 0}))

    for r in rows:
        fx = FX_RATES.get(r["currency"], 1.0)
        cost_brl = r["cost"] * fx
        seg = pub_seg.get(r["pub"], "Long Tail")
        is_net = "vtexads" in r["adv"].lower()
        tri = pub_tr.get(r["pub"], {"tech": 0.1, "net": 0.15})
        tr = tri["net"] if is_net else tri["tech"]
        rev = cost_brl * tr
        key = "N" if is_net else "T"

        seg_delta[seg]["sp"] += cost_brl
        seg_delta[seg]["rv"] += rev
        seg_delta[seg][f"sp{key}"] += cost_brl
        seg_delta[seg][f"rv{key}"] += rev
        pub_delta[seg][r["pub"]]["sp"] += cost_brl
        pub_delta[seg][r["pub"]]["rv"] += rev
        pub_delta[seg][r["pub"]][f"sp{key}"] += cost_brl
        pub_delta[seg][r["pub"]][f"rv{key}"] += rev

    # Advertiser deltas (raw currency for ADV_DATA)
    adv_delta = defaultdict(lambda: {"spend": 0, "pub": "", "curr": ""})
    for r in rows:
        adv_delta[r["adv"]]["spend"] += r["cost"]
        adv_delta[r["adv"]]["pub"] = r["pub"]
        adv_delta[r["adv"]]["curr"] = r["currency"]

    # Daily per-segment (BRL)
    daily_seg = defaultdict(lambda: defaultdict(float))
    for r in rows:
        fx = FX_RATES.get(r["currency"], 1.0)
        seg = pub_seg.get(r["pub"], "Long Tail")
        daily_seg[seg][r["day"].day] += r["cost"] * fx

    # New days (sorted)
    new_days = sorted(daily_brl.keys())

    return {
        "daily_brl": daily_brl,
        "seg_delta": seg_delta,
        "pub_delta": pub_delta,
        "adv_delta": adv_delta,
        "daily_seg": daily_seg,
        "new_days": new_days,
    }


# ─── APPLY UPDATES TO HTML ──────────────────────────────────────────
def apply_updates(html, data, pub_seg, pub_tr):
    """Apply all computed deltas to the dashboard HTML."""
    new_days = data["new_days"]
    daily_brl = data["daily_brl"]
    seg_delta = data["seg_delta"]
    pub_delta = data["pub_delta"]
    adv_delta = data["adv_delta"]
    daily_seg = data["daily_seg"]

    current_na = get_current_na(html)
    new_na = max(new_days)  # e.g., 22 if days 20-22 were added

    if new_na <= current_na:
        print(f"⚠ Data already up to date (NA={current_na}, new max day={new_na}). Skipping.")
        return html

    # Only process days that are actually new
    days_to_add = [d for d in new_days if d > current_na]
    if not days_to_add:
        print("⚠ No new days to add. Skipping.")
        return html

    print(f"  Adding days {days_to_add} (NA: {current_na} → {new_na})")

    # 1. Update NA
    html = re.sub(
        r"const NA\s*=\s*\d+;.*",
        f"const NA = {new_na};  // Actual days in April (1-{new_na})",
        html,
    )

    # 2. Append to ACTUALS
    new_entries = ", ".join(
        [f'{{"day": {d}, "adspend": {round(daily_brl[d])}}}' for d in days_to_add]
    )
    last_actual = re.search(r'(\{"day": ' + str(current_na) + r', "adspend": \d+\})\];', html)
    if last_actual:
        html = html.replace(
            last_actual.group(0),
            last_actual.group(1) + ", " + new_entries + "];",
        )
        print("  ✓ ACTUALS updated")

    # 3. Update REAL_APRIL segments + publishers
    for seg, delta in seg_delta.items():
        pat = rf'"{seg}":\{{spendReal:(\d+),revReal:(\d+),spendTech:(\d+),spendNetwork:(\d+),revTech:(\d+),revNetwork:(\d+)'

        # For "Long Tail" — need to skip the publisher named "Long Tail" and find the segment
        if seg == "Long Tail":
            # Find LATAM block end first
            latam_start = html.find('"LATAM":{spendReal:')
            depth = 0
            latam_end = latam_start
            for i in range(latam_start, len(html)):
                if html[i] == "{": depth += 1
                elif html[i] == "}":
                    depth -= 1
                    if depth == 0:
                        latam_end = i + 1
                        break
            m = re.search(pat, html[latam_end:])
            if m:
                old_str = m.group(0)
                new_str = '"{}":{{spendReal:{},revReal:{},spendTech:{},spendNetwork:{},revTech:{},revNetwork:{}'.format(
                    seg,
                    int(m.group(1)) + round(delta["sp"]),
                    int(m.group(2)) + round(delta["rv"]),
                    int(m.group(3)) + round(delta["spT"]),
                    int(m.group(4)) + round(delta["spN"]),
                    int(m.group(5)) + round(delta["rvT"]),
                    int(m.group(6)) + round(delta["rvN"]),
                )
                html = html[:latam_end] + html[latam_end:].replace(old_str, new_str, 1)
        else:
            m = re.search(pat, html)
            if m:
                old_str = m.group(0)
                new_str = '"{}":{{spendReal:{},revReal:{},spendTech:{},spendNetwork:{},revTech:{},revNetwork:{}'.format(
                    seg,
                    int(m.group(1)) + round(delta["sp"]),
                    int(m.group(2)) + round(delta["rv"]),
                    int(m.group(3)) + round(delta["spT"]),
                    int(m.group(4)) + round(delta["spN"]),
                    int(m.group(5)) + round(delta["rvT"]),
                    int(m.group(6)) + round(delta["rvN"]),
                )
                html = html.replace(old_str, new_str, 1)

        # Update publishers within segment
        if seg in pub_delta:
            for pub, pd in pub_delta[seg].items():
                ppat = rf'"{re.escape(pub)}":\{{spendReal:(\d+),revReal:(\d+),spendTech:(\d+),spendNetwork:(\d+),revTech:(\d+),revNetwork:(\d+)'
                pm = re.search(ppat, html)
                if pm:
                    old_pub = pm.group(0)
                    new_pub = '"{}":{{spendReal:{},revReal:{},spendTech:{},spendNetwork:{},revTech:{},revNetwork:{}'.format(
                        pub,
                        int(pm.group(1)) + round(pd["sp"]),
                        int(pm.group(2)) + round(pd["rv"]),
                        int(pm.group(3)) + round(pd["spT"]),
                        int(pm.group(4)) + round(pd["spN"]),
                        int(pm.group(5)) + round(pd["rvT"]),
                        int(pm.group(6)) + round(pd["rvN"]),
                    )
                    html = html.replace(old_pub, new_pub, 1)

    print("  ✓ REAL_APRIL updated")

    # 4. Update REAL_DAILY_APR — append daily values per segment
    rda_start = html.find("const REAL_DAILY_APR={")
    rda_end = html.find("};", rda_start) + 2
    rda_block = html[rda_start:rda_end]

    for seg in ["Electronics", "Pharma", "LATAM", "Beauty", "Long Tail", "Home Center", "Others", "Groceries"]:
        seg_daily = daily_seg.get(seg, {})
        new_vals = [str(round(seg_daily.get(d, 0))) for d in days_to_add]
        pat = rf'"{seg}":\[([^\]]+)\]'
        m = re.search(pat, rda_block)
        if m:
            old_arr = m.group(1)
            new_arr = old_arr + "," + ",".join(new_vals)
            rda_block = rda_block.replace(f'"{seg}":[{old_arr}]', f'"{seg}":[{new_arr}]', 1)

    html = html[:rda_start] + rda_block + html[rda_end:]
    print("  ✓ REAL_DAILY_APR updated")

    # 5. Update REAL_MONTHLY 2026-04 entries
    for seg in seg_delta:
        seg_rm = seg  # same key in REAL_MONTHLY
        pat = rf'"{seg_rm}":(.*?"2026-04":\{{spend:(\d+),rev:(\d+)\}})'
        m = re.search(pat, html)
        if m:
            old_sp = int(m.group(2))
            old_rv = int(m.group(3))
            delta = seg_delta[seg]
            old_str = f'"2026-04":{{spend:{old_sp},rev:{old_rv}}}'
            new_str = f'"2026-04":{{spend:{old_sp + round(delta["sp"])},rev:{old_rv + round(delta["rv"])}}}'
            rm_start = html.find(f'"{seg_rm}":', html.find("const REAL_MONTHLY"))
            if rm_start != -1:
                rm_end = html.find("},", rm_start) + 2
                rm_block = html[rm_start:rm_end]
                rm_block = rm_block.replace(old_str, new_str, 1)
                html = html[:rm_start] + rm_block + html[rm_end:]

    print("  ✓ REAL_MONTHLY updated")

    # 6. Update ADV_DATA
    adv_m = re.search(r"const ADV_DATA = (\[.*?\]);", html, re.DOTALL)
    if adv_m:
        adv_data = json.loads(adv_m.group(1))
        adv_by_name = {a["n"]: a for a in adv_data}

        updated = added = 0
        for adv_name, nd in adv_delta.items():
            raw_spend = round(nd["spend"])
            if adv_name in adv_by_name:
                a = adv_by_name[adv_name]
                a["sp"] += raw_spend
                a["apr"] += raw_spend
                is_net = "vtexads" in adv_name.lower()
                a["st"] = 0 if is_net else a["apr"]
                a["sn"] = a["apr"] if is_net else 0
                updated += 1
            else:
                seg = pub_seg.get(nd["pub"], "Long Tail")
                is_net = "vtexads" in adv_name.lower()
                tri = pub_tr.get(nd["pub"], {"tech": 0.1, "net": 0.15})
                tr = tri["net"] if is_net else tri["tech"]
                adv_data.append({
                    "n": adv_name, "sp": raw_spend,
                    "st": 0 if is_net else raw_spend,
                    "sn": raw_spend if is_net else 0,
                    "pub": nd["pub"], "seg": seg, "tr": tr,
                    "status": "new", "avg30": 0,
                    "jan": 0, "feb": 0, "mar": 0, "apr": raw_spend,
                })
                added += 1

        new_adv_json = json.dumps(adv_data, ensure_ascii=False)
        html = html.replace(adv_m.group(0), f"const ADV_DATA = {new_adv_json};", 1)
        print(f"  ✓ ADV_DATA: {updated} updated, {added} added ({len(adv_data)} total)")

    return html


# ─── MAIN ────────────────────────────────────────────────────────────
def main():
    # Determine date range
    if len(sys.argv) >= 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        yesterday = datetime.now() - timedelta(days=1)
        start_date = end_date = yesterday.strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"  VTEX Ads Forecast — Daily Update")
    print(f"  Date range: {start_date} → {end_date}")
    print(f"{'='*60}\n")

    # 1. Auth
    token = metabase_auth()

    # 2. Fetch
    raw_data = fetch_data(token, start_date, end_date)
    if not raw_data:
        print("⚠ No data returned. Exiting.")
        return

    # 3. Load HTML + extract mappings
    html = load_html()
    pub_seg, pub_tr = extract_pub_mapping(html)

    # 4. Process
    data = process_rows(raw_data, pub_seg, pub_tr)

    # 5. Apply
    html = apply_updates(html, data, pub_seg, pub_tr)

    # 6. Save
    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n✓ Dashboard saved to {HTML_PATH}")
    print(f"  New NA = {get_current_na(html)}")


if __name__ == "__main__":
    main()

