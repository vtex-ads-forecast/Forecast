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
SETTINGS_PATH = os.path.join(os.path.dirname(__file__), "..", "settings.json")

# ⚠️ CRITICAL: The "Others" tab and "Others" segment are MANUALLY managed by the user.
# NEVER modify, reset, or touch them in ANY automated script or code change.
# This includes: othersStore, othersData, localStorage Others, MONTHS_DATA.others,
# and the "Others" segment in REAL_APRIL / REAL_MONTHLY.
# Only the user can modify these values through the dashboard UI.
PROTECTED_SEGMENTS = {"Others"}

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

    # Use /api/dataset with raw SQL + LIMIT/OFFSET pagination to bypass 2000 row cap
    DB_ID = 13  # from card info
    PAGE_SIZE = 2000

    base_sql = f"""
    with metrics AS (
        SELECT cmnd.day, cmnd.campaign_id, cmnd.publisher_id, cmnd.advertiser_id,
               SUM(cmnd.total_clicks_cost) + SUM(cmnd.total_impressions_cost) AS total_cost,
               p.name AS publisher_name, p.currency_code, p.is_test
        FROM CAMPAIGNS_METRICS_NETWORK_DAY cmnd
        JOIN publishers p ON p.id = cmnd.publisher_id
        WHERE day BETWEEN ('{start_date}')::timestamp - INTERVAL '1 day'
                      AND ('{end_date}')::timestamp + INTERVAL '2 days'
        GROUP BY cmnd.day, cmnd.campaign_id, cmnd.publisher_id, cmnd.advertiser_id,
                 p.name, p.currency_code, p.is_test
    ),
    costs AS (
        SELECT r.day, r.publisher_id, r.publisher_name, r.advertiser_id,
               a.name AS advertiser_name, r.currency_code, r.campaign_id,
               SUM(r.total_cost) AS total_cost
        FROM metrics r
        LEFT JOIN advertisers a ON a.id = r.advertiser_id
        WHERE r.day >= '{start_date}'
          AND r.day < '{end_date}'::date + INTERVAL '1 day'
          AND r.is_test = false
        GROUP BY r.day, r.publisher_id, r.publisher_name, r.advertiser_id,
                 a.name, r.currency_code, r.campaign_id
    )
    SELECT day, publisher_id, publisher_name, advertiser_id, advertiser_name,
           currency_code, campaign_id, total_cost
    FROM costs WHERE total_cost > 0 ORDER BY day DESC, publisher_id, advertiser_id
    """

    all_rows = []
    cols = []
    offset = 0

    while True:
        paginated_sql = f"{base_sql} LIMIT {PAGE_SIZE} OFFSET {offset}"
        print(f"  Fetching offset={offset} limit={PAGE_SIZE}...")
        resp = requests.post(
            f"{METABASE_URL}/api/dataset",
            headers=headers,
            json={
                "database": DB_ID,
                "type": "native",
                "native": {"query": paginated_sql},
            },
            timeout=300,
        )
        resp.raise_for_status()
        result = resp.json()

        if not cols:
            cols = [c.get("name", f"col_{i}") for i, c in enumerate(result.get("data", {}).get("cols", []))]
            print(f"  Columns: {cols}")

        page_rows = result.get("data", {}).get("rows", [])
        print(f"  Got {len(page_rows)} rows")
        all_rows.extend(page_rows)

        if len(page_rows) < PAGE_SIZE:
            break  # last page

        offset += PAGE_SIZE
        if offset > 50000:  # safety limit
            print("  ⚠ Safety limit reached at 50000 rows")
            break

    rows_raw = all_rows
    print(f"  Total rows fetched: {len(rows_raw)}")
    if rows_raw:
        print(f"  Sample row: {rows_raw[0]}")

    if not rows_raw:
        print("⚠ No rows returned from Metabase")
        return []

    # Build column index mapping (rows are arrays from /query endpoint)
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

    # Convert rows (arrays) to standardized dicts
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


# ─── SETTINGS (shared TR overrides) ──────────────────────────────────
def load_settings():
    """Load settings.json if it exists. Returns { pub: {trTech, trNetwork, seg} }."""
    if not os.path.exists(SETTINGS_PATH):
        print("  ℹ No settings.json found — using HTML defaults")
        return {}
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            settings = json.load(f)
        print(f"✓ Loaded settings.json ({len(settings)} publishers)")
        return settings
    except Exception as e:
        print(f"  ⚠ Failed to load settings.json: {e}")
        return {}


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

    print(f"✓ Extracted {len(pub_seg)} publisher mappings from HTML")

    # settings.json is the PRIMARY source of truth for segments and TRs
    settings = load_settings()
    added = 0
    overrides = 0
    for pname, ov in settings.items():
        # Add ALL publishers from settings.json (not just those already in REAL_APRIL)
        if pname not in pub_seg:
            if ov.get("seg"):
                pub_seg[pname] = ov["seg"]
            pub_tr[pname] = {
                "tech": ov.get("trTech", 0.1),
                "net": ov.get("trNetwork", 0.15),
            }
            added += 1
        else:
            # Override existing entries
            if ov.get("trTech") is not None:
                pub_tr[pname]["tech"] = ov["trTech"]
            if ov.get("trNetwork") is not None:
                pub_tr[pname]["net"] = ov["trNetwork"]
            if ov.get("seg"):
                pub_seg[pname] = ov["seg"]
            overrides += 1
    if added:
        print(f"  ✓ Added {added} publisher mappings from settings.json")
    if overrides:
        print(f"  ✓ Applied {overrides} TR overrides from settings.json")

    # Also extract mappings from META_APRIL as backup
    meta_start = html.find("const META_APRIL=")
    if meta_start != -1:
        meta_depth = 0
        meta_end = meta_start
        for i in range(html.find("{", meta_start), len(html)):
            if html[i] == "{": meta_depth += 1
            elif html[i] == "}":
                meta_depth -= 1
                if meta_depth == 0:
                    meta_end = i + 1
                    break
        meta_block = html[meta_start:meta_end]
        meta_added = 0
        for seg_m in re.finditer(r'"([^"]+)":\{spendMeta:', meta_block):
            seg_name = seg_m.group(1)
            # Find publishers in this META segment
            seg_pos = seg_m.start()
            pub_section = meta_block.find("publishers:{", seg_pos)
            if pub_section == -1:
                continue
            inner = pub_section + len("publishers:{")
            d2 = 1
            inner_end = inner
            for i2 in range(inner, len(meta_block)):
                if meta_block[i2] == "{": d2 += 1
                elif meta_block[i2] == "}":
                    d2 -= 1
                    if d2 == 0:
                        inner_end = i2
                        break
            pub_inner = meta_block[inner:inner_end]
            for pm2 in re.finditer(r'"([^"]+)":\{spendMeta:', pub_inner):
                mp = pm2.group(1)
                if mp not in pub_seg:
                    pub_seg[mp] = seg_name
                    tr_t = re.search(r'trTech:([\d.]+)', pub_inner[pm2.start():])
                    tr_n = re.search(r'trNetwork:([\d.]+)', pub_inner[pm2.start():])
                    pub_tr[mp] = {
                        "tech": float(tr_t.group(1)) if tr_t else 0.1,
                        "net": float(tr_n.group(1)) if tr_n else 0.15,
                    }
                    meta_added += 1
        if meta_added:
            print(f"  ✓ Added {meta_added} publisher mappings from META_APRIL")

    print(f"  Total publisher mappings: {len(pub_seg)}")
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
        # Never touch "Others" segment — it is manually managed only
        if seg == "Others":
            seg = "Long Tail"
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

    # Advertiser deltas (BRL-converted, consistent with seg_delta/pub_delta)
    adv_delta = defaultdict(lambda: {"spend": 0, "pub": "", "curr": ""})
    for r in rows:
        fx = FX_RATES.get(r["currency"], 1.0)
        adv_delta[r["adv"]]["spend"] += r["cost"] * fx
        adv_delta[r["adv"]]["pub"] = r["pub"]
        adv_delta[r["adv"]]["curr"] = r["currency"]

    # Daily per-segment (BRL)
    daily_seg = defaultdict(lambda: defaultdict(float))
    for r in rows:
        fx = FX_RATES.get(r["currency"], 1.0)
        seg = pub_seg.get(r["pub"], "Long Tail")
        # Never touch "Others" segment — it is manually managed only
        if seg == "Others":
            seg = "Long Tail"
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


# ─── MONTH TRANSITION ──────────────────────────────────────────────
MONTH_NAMES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}

def get_current_month_from_html(html):
    """Detect which month is currently 'current' in the dashboard."""
    m = re.search(r'"(\d{4}-\d{2})":\s*\{\s*status:\s*"current"', html)
    return m.group(1) if m else None

def days_in_month(year, month):
    """Return the number of days in a given month."""
    import calendar
    return calendar.monthrange(year, month)[1]

def close_month_and_open_new(html, new_month_key):
    """
    Close the current month and open a new one.
    Steps:
    1. Change current month status from "current" to "closed" in MONTHS_DATA
    2. Copy REAL_APRIL → REAL_CLOSED_NEW and add to CLOSED_DETAIL
    3. Build cumulative daily spend/rev arrays for the closing month
    4. Reset REAL_APRIL to empty segments (preserve structure from META_APRIL)
    5. Reset NA=0, TD=new month days, ACTUALS=[]
    6. Reset REAL_DAILY_APR to empty arrays
    7. Reset ADV_DATA: move current month spend to historical, zero out sn/st
    8. Add new month entry to MONTHS_DATA as "current"
    """
    cur_month = get_current_month_from_html(html)
    if not cur_month:
        print("  ⚠ No current month found in MONTHS_DATA")
        return html

    cur_year, cur_mon = int(cur_month[:4]), int(cur_month[5:7])
    new_year, new_mon = int(new_month_key[:4]), int(new_month_key[5:7])
    new_td = days_in_month(new_year, new_mon)
    new_label = f"{MONTH_NAMES[new_mon]} {new_year}"

    print(f"\n{'='*60}")
    print(f"  MONTH TRANSITION: {cur_month} → {new_month_key}")
    print(f"  New month: {new_label} ({new_td} days)")
    print(f"{'='*60}")

    # --- Step 1: Close current month in MONTHS_DATA ---
    # Change status: "current" to "closed" and fix computed fields
    cur_na = get_current_na(html)
    # Get REALIZED_TOTAL from ACTUALS
    actuals_m = re.search(r'const ACTUALS = \[(.*?)\];', html)
    actuals_total = 0
    if actuals_m and actuals_m.group(1).strip():
        for dm in re.finditer(r'"adspend":\s*(\d+)', actuals_m.group(1)):
            actuals_total += int(dm.group(1))

    # Read current META totals
    meta_sp_m = re.search(r'const META_SPEND_TOTAL\s*=\s*(\d+)', html)
    meta_rv_m = re.search(r'const META_REV_TOTAL\s*=\s*(\d+)', html)
    meta_sp = int(meta_sp_m.group(1)) if meta_sp_m else 0
    meta_rv = int(meta_rv_m.group(1)) if meta_rv_m else 0

    # Read blended TR for revenue estimate
    blended_m = re.search(r'BLENDED_TR\s*=\s*([\d.]+)', html)
    blended_tr = float(blended_m.group(1)) if blended_m else 0.1
    actuals_rev = round(actuals_total * blended_tr)

    # Replace the current month block in MONTHS_DATA
    old_current = re.search(
        rf'"{cur_month}":\s*\{{[^}}]*status:\s*"current"[^}}]*\}}',
        html
    )
    if old_current:
        new_closed = (
            f'"{cur_month}":{{status:"closed",label:"{MONTH_NAMES[cur_mon]} {cur_year}",'
            f'days:{cur_na},actualDays:{cur_na},'
            f'metaSpend:{meta_sp},metaRev:{meta_rv},'
            f'realSpend:{actuals_total},realRev:{actuals_rev},'
            f'segMetas:null,others:[],forecastUsed:null}}'
        )
        html = html[:old_current.start()] + new_closed + html[old_current.end():]
        print(f"  ✓ Closed {cur_month} in MONTHS_DATA (realSpend={actuals_total:,})")

    # --- Step 2: Copy REAL_APRIL → CLOSED_DETAIL ---
    # Create a new const for the closed month's detail
    ra_m = re.search(r'(const REAL_APRIL\s*=\s*\{)', html)
    ra_start = ra_m.start(1)
    depth = 0
    for i in range(html.find('{', ra_start), len(html)):
        if html[i] == '{': depth += 1
        elif html[i] == '}': depth -= 1
        if depth == 0:
            ra_end = i + 1
            break
    ra_block = html[ra_start:ra_end]
    closed_var = f'REAL_CLOSED_{cur_month.replace("-","_")}'

    # Extract just the value (everything after the =)
    eq_pos = ra_block.find('=')
    ra_value = ra_block[eq_pos + 1:].strip()  # The {...} object

    # Insert the closed data before CLOSED_DETAIL
    cd_pos = html.find('const CLOSED_DETAIL')
    closed_const = f'const {closed_var} = {ra_value};\n\n'
    html = html[:cd_pos] + closed_const + html[cd_pos:]

    # Add to CLOSED_DETAIL
    cd_m = re.search(r'const CLOSED_DETAIL\s*=\s*\{', html)
    insert_pos = html.find('}', cd_m.end())
    new_entry = f',\n  "{cur_month}": {{ real: {closed_var} }}'
    # Check if CLOSED_DETAIL is empty
    block = html[cd_m.end():insert_pos].strip()
    if block:
        html = html[:insert_pos] + new_entry + html[insert_pos:]
    else:
        html = html[:insert_pos] + f'\n  "{cur_month}": {{ real: {closed_var} }}' + html[insert_pos:]
    print(f"  ✓ Saved {cur_month} detail as {closed_var}")

    # --- Step 3: Build cumulative daily arrays ---
    if actuals_m and actuals_m.group(1).strip():
        day_spend = {}
        for dm in re.finditer(r'\{"day":\s*(\d+),\s*"adspend":\s*(\d+)\}', actuals_m.group(1)):
            day_spend[int(dm.group(1))] = int(dm.group(2))

        cumul_spend = []
        running = 0
        for d in sorted(day_spend.keys()):
            running += day_spend[d]
            cumul_spend.append(running)

        # For revenue, approximate with blended TR
        cumul_rev = [round(s * blended_tr) for s in cumul_spend]

        # Add to MONTHLY_DAILY_CUMUL_SPEND
        cumul_sp_m = re.search(r'(const MONTHLY_DAILY_CUMUL_SPEND\s*=\s*\{)', html)
        close_brace = html.find('};', cumul_sp_m.end())
        # Check if previous entry already has trailing comma
        pre = html[:close_brace].rstrip()
        sep = '' if pre.endswith(',') else ','
        sp_entry = f'{sep}\n  "{cur_month}": [{",".join(str(x) for x in cumul_spend)}]\n'
        html = html[:close_brace] + sp_entry + html[close_brace:]

        cumul_rv_m = re.search(r'(const MONTHLY_DAILY_CUMUL_REV\s*=\s*\{)', html)
        close_brace2 = html.find('};', cumul_rv_m.end())
        pre2 = html[:close_brace2].rstrip()
        sep2 = '' if pre2.endswith(',') else ','
        rv_entry = f'{sep2}\n  "{cur_month}": [{",".join(str(x) for x in cumul_rev)}]\n'
        html = html[:close_brace2] + rv_entry + html[close_brace2:]
        print(f"  ✓ Added cumulative daily arrays for {cur_month}")

    # --- Step 4: Reset REAL_APRIL ---
    # Get TOP-LEVEL segment names from REAL_APRIL (not publisher names inside them)
    # Top-level segments are at depth 1 inside the REAL_APRIL object.
    # We parse by finding keys that appear right after the opening { or after a }},
    ra_m3 = re.search(r'const REAL_APRIL\s*=\s*\{', html)
    ra_block_text = html[ra_m3.start():]
    seg_names = []
    # Walk the structure at depth 1
    brace_pos = ra_block_text.find('{')
    depth = 0
    i = brace_pos
    while i < len(ra_block_text):
        c = ra_block_text[i]
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                break
        elif c == '"' and depth == 1:
            # Found a top-level key
            end_q = ra_block_text.find('"', i + 1)
            seg_names.append(ra_block_text[i+1:end_q])
            # Skip to after this segment's value
            colon = ra_block_text.find(':', end_q)
            # Find the opening { of the segment value
            seg_brace = ra_block_text.find('{', colon)
            d2 = 0
            j = seg_brace
            while j < len(ra_block_text):
                if ra_block_text[j] == '{': d2 += 1
                elif ra_block_text[j] == '}': d2 -= 1
                if d2 == 0:
                    i = j + 1
                    break
                j += 1
            continue
        i += 1

    empty_segs = {}
    for seg in seg_names:
        empty_segs[seg] = '{spendReal:0,revReal:0,spendTech:0,spendNetwork:0,revTech:0,revNetwork:0,publishers:{}}'

    new_ra = 'const REAL_APRIL={' + ',\n'.join(
        f'"{s}":{v}' for s, v in empty_segs.items()
    ) + '}'

    # Need to re-find REAL_APRIL position (it may have shifted after inserts)
    ra_m2 = re.search(r'const REAL_APRIL\s*=\s*\{', html)
    first_brace = html.find('{', ra_m2.start())
    depth = 0
    for i in range(first_brace, len(html)):
        if html[i] == '{': depth += 1
        elif html[i] == '}':
            depth -= 1
            if depth == 0:
                ra_end2 = i + 1
                break
    html = html[:ra_m2.start()] + new_ra + html[ra_end2:]
    print(f"  ✓ Reset REAL_APRIL ({len(seg_names)} segments)")

    # --- Step 5: Reset NA, TD, ACTUALS ---
    html = re.sub(r'const NA\s*=\s*\d+;.*', f'const NA = 0;  // Actual days in current month (1-0)', html)
    html = re.sub(r'const TD\s*=\s*\d+;.*', f'const TD = {new_td};  // Total {MONTH_NAMES[new_mon]} days', html)
    html = re.sub(r'const ACTUALS\s*=\s*\[.*?\];', 'const ACTUALS = [];', html)
    print(f"  ✓ Reset NA=0, TD={new_td}, ACTUALS=[]")

    # --- Step 6: Reset REAL_DAILY_APR ---
    rda_m = re.search(r'const REAL_DAILY_APR\s*=\s*\{', html)
    if rda_m:
        rda_brace = html.find('{', rda_m.start())
        depth = 0
        for i in range(rda_brace, len(html)):
            if html[i] == '{': depth += 1
            elif html[i] == '}':
                depth -= 1
                if depth == 0:
                    rda_end = i + 1
                    break
        empty_rda = 'const REAL_DAILY_APR = {' + ','.join(f'"{s}":[]' for s in seg_names) + '}'
        html = html[:rda_m.start()] + empty_rda + html[rda_end:]
        print(f"  ✓ Reset REAL_DAILY_APR")

    # --- Step 7: Reset ADV_DATA ---
    month_abbr = {1:"jan",2:"feb",3:"mar",4:"apr",5:"may",6:"jun",7:"jul",8:"aug",9:"sep",10:"oct",11:"nov",12:"dec"}
    cur_abbr = month_abbr[cur_mon]

    adv_m = re.search(r"const ADV_DATA = (\[.*?\]);", html, re.DOTALL)
    if adv_m:
        adv_data = json.loads(adv_m.group(1))
        for a in adv_data:
            # Move current month to historical field, zero out running fields
            a["sp"] = 0
            a["st"] = 0
            a["sn"] = 0
        new_adv_json = json.dumps(adv_data, ensure_ascii=False)
        html = html.replace(adv_m.group(0), f"const ADV_DATA = {new_adv_json};", 1)
        print(f"  ✓ Reset ADV_DATA ({len(adv_data)} advertisers)")

    # --- Step 8: Add new month to MONTHS_DATA ---
    # Find MONTHS_DATA block and its closing };
    md_m = re.search(r'const MONTHS_DATA\s*=\s*\{', html)
    md_brace_start = html.find('{', md_m.start())
    depth = 0
    md_end = md_brace_start
    for i in range(md_brace_start, len(html)):
        if html[i] == '{': depth += 1
        elif html[i] == '}':
            depth -= 1
            if depth == 0:
                md_end = i
                break

    new_month_entry = (
        f',\n  "{new_month_key}": {{\n'
        f'    status: "current", label: "{new_label}", days: TD, actualDays: NA,\n'
        f'    metaSpend: META_SPEND_TOTAL, metaRev: META_REV_TOTAL,\n'
        f'    realSpend: REALIZED_TOTAL,\n'
        f'    realRev: REALIZED_TOTAL * BLENDED_TR,\n'
        f'    segMetas: null,\n'
        f'    others: null,\n'
        f'    forecastUsed: null\n'
        f'  }}'
    )
    html = html[:md_end] + new_month_entry + '\n' + html[md_end:]
    print(f"  ✓ Added {new_month_key} to MONTHS_DATA as current")

    print(f"\n  ✓ Month transition complete: {cur_month} → {new_month_key}")
    return html


# ─── APPLY UPDATES TO HTML ──────────────────────────────────────────
def apply_updates(html, data, pub_seg, pub_tr, start_date="2026-05-01"):
    """Apply all computed deltas to the dashboard HTML."""
    new_days = data["new_days"]
    daily_brl = data["daily_brl"]
    seg_delta = data["seg_delta"]
    pub_delta = data["pub_delta"]
    adv_delta = data["adv_delta"]
    daily_seg = data["daily_seg"]

    current_na = get_current_na(html)
    new_na = max(new_days)  # e.g., 22 if days 20-22 were added

    # Check for --force flag or FORCE_UPDATE env var
    force = "--force" in sys.argv or os.environ.get("FORCE_UPDATE") == "1"

    # Find which days are missing from ACTUALS (gaps)
    actuals_m = re.search(r'const ACTUALS = \[(.*?)\];', html)
    existing_days = set()
    if actuals_m and actuals_m.group(1).strip():
        for dm in re.finditer(r'"day":\s*(\d+)', actuals_m.group(1)):
            existing_days.add(int(dm.group(1)))

    missing_days = [d for d in new_days if d not in existing_days]

    if not missing_days and new_na <= current_na and not force:
        print(f"⚠ Data already up to date (NA={current_na}, all days present). Use --force to re-process.")
        return html

    if force:
        days_to_add = new_days
        print(f"  Forcing re-process for days {new_days}")
    elif missing_days:
        days_to_add = missing_days
        print(f"  Filling gap days: {missing_days}")
    else:
        days_to_add = [d for d in new_days if d > current_na]

    if not days_to_add:
        print("⚠ No new days to add. Skipping.")
        return html

    # NA = highest day we have data for
    new_na = max(new_na, current_na)
    print(f"  Adding days {days_to_add} (NA: {current_na} → {new_na})")

    # 1. Update NA
    html = re.sub(
        r"const NA\s*=\s*\d+;.*",
        f"const NA = {new_na};  // Actual days in current month (1-{new_na})",
        html,
    )

    # 2. Update ACTUALS — rebuild with all days sorted
    actuals_m2 = re.search(r'const ACTUALS = \[(.*?)\];', html)
    if actuals_m2:
        # Parse existing entries
        all_actuals = {}
        for dm in re.finditer(r'\{"day":\s*(\d+),\s*"adspend":\s*(\d+)\}', actuals_m2.group(1)):
            all_actuals[int(dm.group(1))] = int(dm.group(2))
        # Add/update new days
        for d in days_to_add:
            if d not in all_actuals or force:
                all_actuals[d] = round(daily_brl[d])
        # Rebuild sorted
        sorted_days = sorted(all_actuals.keys())
        entries_str = ", ".join(
            [f'{{"day": {d}, "adspend": {all_actuals[d]}}}' for d in sorted_days]
        )
        html = html.replace(actuals_m2.group(0), f"const ACTUALS = [{entries_str}];")
        print(f"  ✓ ACTUALS updated ({len(sorted_days)} days)")

    # 3. Update REAL_APRIL segments + publishers
    for seg, delta in seg_delta.items():
        # NEVER touch protected segments
        if seg in PROTECTED_SEGMENTS:
            print(f"  ⚠ Skipping protected segment: {seg}")
            continue
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
                else:
                    # New publisher — add to the segment's publishers:{} block
                    tri = pub_tr.get(pub, {"tech": 0.1, "net": 0.15})
                    new_pub_entry = '"{}":{{spendReal:{},revReal:{},spendTech:{},spendNetwork:{},revTech:{},revNetwork:{},trTech:{},trNetwork:{}}}'.format(
                        pub, round(pd["sp"]), round(pd["rv"]),
                        round(pd["spT"]), round(pd["spN"]),
                        round(pd["rvT"]), round(pd["rvN"]),
                        tri["tech"], tri["net"],
                    )
                    # Find the publishers:{} block for this segment
                    seg_pat = rf'"{re.escape(seg)}":\{{spendReal:'
                    seg_pos = html.find(f'"{seg}":{{spendReal:')
                    if seg == "Long Tail":
                        # Special handling: find Long Tail after LATAM
                        latam_s = html.find('"LATAM":{spendReal:')
                        d2 = 0
                        for ii in range(latam_s, len(html)):
                            if html[ii] == "{": d2 += 1
                            elif html[ii] == "}":
                                d2 -= 1
                                if d2 == 0:
                                    seg_pos = html.find('"Long Tail":{spendReal:', ii)
                                    break
                    if seg_pos != -1:
                        pub_block_start = html.find("publishers:{", seg_pos)
                        if pub_block_start != -1:
                            pub_inner = pub_block_start + len("publishers:{")
                            # Check if publishers block is empty
                            if html[pub_inner] == "}":
                                html = html[:pub_inner] + new_pub_entry + html[pub_inner:]
                            else:
                                html = html[:pub_inner] + new_pub_entry + "," + html[pub_inner:]
                            print(f"    + Added new publisher: {pub} → {seg}")

    print("  ✓ REAL_APRIL updated")

    # 4. Update REAL_DAILY_APR — append daily values per segment (dynamic)
    rda_start = html.find("const REAL_DAILY_APR={")
    rda_end = html.find("};", rda_start) + 2
    rda_block = html[rda_start:rda_end]

    # Extract all segment names dynamically from REAL_DAILY_APR
    rda_segs = re.findall(r'"([^"]+)":\[', rda_block)
    for seg in rda_segs:
        seg_daily = daily_seg.get(seg, {})
        new_vals = [str(round(seg_daily.get(d, 0))) for d in days_to_add]
        pat = rf'"{re.escape(seg)}":\[([^\]]*)\]'
        m = re.search(pat, rda_block)
        if m:
            old_arr = m.group(1)
            if old_arr:
                new_arr = old_arr + "," + ",".join(new_vals)
            else:
                new_arr = ",".join(new_vals)
            rda_block = rda_block.replace(m.group(0), f'"{seg}":[{new_arr}]', 1)

    html = html[:rda_start] + rda_block + html[rda_end:]
    print("  ✓ REAL_DAILY_APR updated")

    # 5. Update REAL_MONTHLY entries for current month
    # Detect month from the data being processed
    current_month_key = f"{days_to_add[0]:02d}" if False else None
    # Use the actual date from the data
    data_month = datetime(int(start_date[:4]), int(start_date[5:7]), 1)
    month_key = data_month.strftime("%Y-%m")
    print(f"  Updating REAL_MONTHLY for {month_key}")

    for seg in seg_delta:
        if seg in PROTECTED_SEGMENTS:
            continue
        seg_rm = seg
        rm_start_search = html.find("const REAL_MONTHLY")
        seg_start = html.find(f'"{seg_rm}":', rm_start_search)
        if seg_start == -1:
            continue

        # Find the closing of this segment's monthly object
        depth = 0
        seg_obj_start = html.find("{", seg_start + len(f'"{seg_rm}":'))
        seg_obj_end = seg_obj_start
        for i in range(seg_obj_start, len(html)):
            if html[i] == "{": depth += 1
            elif html[i] == "}":
                depth -= 1
                if depth == 0:
                    seg_obj_end = i
                    break

        seg_block = html[seg_obj_start:seg_obj_end + 1]
        delta = seg_delta[seg]

        if f'"{month_key}"' in seg_block:
            # Update existing entry
            pat = rf'"{month_key}":\{{spend:(\d+),rev:(\d+)\}}'
            m = re.search(pat, seg_block)
            if m:
                old_sp = int(m.group(1))
                old_rv = int(m.group(2))
                old_str = f'"{month_key}":{{spend:{old_sp},rev:{old_rv}}}'
                new_str = f'"{month_key}":{{spend:{old_sp + round(delta["sp"])},rev:{old_rv + round(delta["rv"])}}}'
                new_block = seg_block.replace(old_str, new_str, 1)
                html = html[:seg_obj_start] + new_block + html[seg_obj_end + 1:]
        else:
            # Add new entry
            insert = f',"{month_key}":{{spend:{round(delta["sp"])},rev:{round(delta["rv"])}}}'
            html = html[:seg_obj_end] + insert + html[seg_obj_end:]

    print("  ✓ REAL_MONTHLY updated")

    # 6. Update ADV_DATA (dynamic month key)
    month_abbr_map = {1:"jan",2:"feb",3:"mar",4:"apr",5:"may",6:"jun",7:"jul",8:"aug",9:"sep",10:"oct",11:"nov",12:"dec"}
    cur_month_num = int(start_date[5:7])
    cur_month_key = month_abbr_map.get(cur_month_num, "may")

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
                a[cur_month_key] = a.get(cur_month_key, 0) + raw_spend
                is_net = "vtexads" in adv_name.lower()
                a["st"] = 0 if is_net else a.get(cur_month_key, 0)
                a["sn"] = a.get(cur_month_key, 0) if is_net else 0
                updated += 1
            else:
                seg = pub_seg.get(nd["pub"], "Long Tail")
                is_net = "vtexads" in adv_name.lower()
                tri = pub_tr.get(nd["pub"], {"tech": 0.1, "net": 0.15})
                tr = tri["net"] if is_net else tri["tech"]
                new_adv = {
                    "n": adv_name, "sp": raw_spend,
                    "st": 0 if is_net else raw_spend,
                    "sn": raw_spend if is_net else 0,
                    "pub": nd["pub"], "seg": seg, "tr": tr,
                    "status": "new", "avg30": 0,
                    "jan": 0, "feb": 0, "mar": 0, "apr": 0,
                }
                new_adv[cur_month_key] = raw_spend
                adv_data.append(new_adv)
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

    # 3b. Check if we need a month transition
    data_month_key = f"{start_date[:4]}-{start_date[5:7]}"
    current_month_key = get_current_month_from_html(html)
    if current_month_key and data_month_key != current_month_key:
        print(f"\n⚡ Data month ({data_month_key}) differs from dashboard month ({current_month_key})")
        html = close_month_and_open_new(html, data_month_key)
        # Save intermediate state before applying new data
        with open(HTML_PATH, "w", encoding="utf-8") as f:
            f.write(html)
        print("  ✓ Intermediate save after month transition")

    pub_seg, pub_tr = extract_pub_mapping(html)

    # 4. Process
    data = process_rows(raw_data, pub_seg, pub_tr)

    # 5. Apply
    html = apply_updates(html, data, pub_seg, pub_tr, start_date)

    # 6. Save
    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n✓ Dashboard saved to {HTML_PATH}")
    print(f"  New NA = {get_current_na(html)}")


if __name__ == "__main__":
    main()

