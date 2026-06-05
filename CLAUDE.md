# VTEX Ads Forecast Dashboard — Project Context

## Overview
Single-file dashboard (`index.html`) hosted on GitHub Pages that shows ad spend forecasting for VTEX Ads. Data is pulled daily from Metabase via GitHub Actions and embedded directly in the HTML.

**Repo:** `vtex-ads-forecast/Forecast` on GitHub  
**Live URL:** GitHub Pages (auto-deploys on push to main via `.github/workflows/deploy-pages.yml`)

## Architecture

### Data Flow
1. **Metabase** (metabase.newtail.com.br) → raw advertiser/publisher spend data
2. **GitHub Actions** (`scripts/update.py`) runs daily at 09:00 UTC, fetches D-1 data, updates `index.html`
3. **GitHub Pages** serves the updated dashboard
4. **Others tab** (manual entries) → saved to `others.json` via GitHub Contents API from the browser

### Key Files
- `index.html` — The entire dashboard (single-file app, ~3500 lines)
- `others.json` — Manual revenue entries (Offsite+Instore, Fees), auto-committed from dashboard UI
- `settings.json` — Publisher TR overrides and segment reassignments
- `scripts/update.py` — Daily data updater (Metabase → HTML)
- `scripts/export_excel.py` — Export historical data to Excel (runs via GitHub Actions)
- `.github/workflows/daily-update.yml` — Daily cron + manual dispatch
- `.github/workflows/deploy-pages.yml` — Auto-deploy on push to main
- `.github/workflows/export-excel.yml` — Manual dispatch to generate Excel export

### GitHub Auto-Commit (from browser)
The dashboard can commit `others.json` directly via GitHub Contents API using a fine-grained PAT stored in `localStorage` key `vtex_ads_gh_token`. Debounced 2s after any Others edit.

## Data Structures in index.html

### Current Month (May 2026)
- `REAL_APRIL` — Publisher-level realized data with tech/network split (despite the name, it's the CURRENT month)
- `ACTUALS` — Daily total ad spend array `[{day, adspend}]`
- `ADV_DATA` — Advertiser-level data (787 advertisers) with `{n, sp, st, sn, pub, seg, tr, status, jan, feb, mar, apr, may}`
- `ADV_CHURN` — 121 churned advertisers
- `META_APRIL` — Monthly targets (meta) per segment with publisher-level breakdown
- `REAL_DAILY_APR` — Daily spend per segment array
- `NA` — Number of actual days with data in current month
- `TD` — Total days in current month

### Historical Data
- `REAL_MONTHLY` — Segment-level spend/rev per month (Jan-May), format: `{segment: {"YYYY-MM": {spend, rev}}}`
- `REAL_CLOSED_APR` — April closed month with full publisher detail (tech/network split)
- `CLOSED_DETAIL` — Maps closed months to their detail data
- `MONTHLY_DAILY_CUMUL_SPEND/REV` — Cumulative daily arrays per month (for MTD comparison)

### Display Segments (consolidated from META/REAL)
```
DISPLAY_SEGMENTS / DISPLAY_REAL keys:
  Beauty, Electronics, Pharma, Groceries, Home Center, LATAM,
  Long Tail, Other Segments, Offsite + Instore, Fees
```

### Segment Mappings
- `Other Segments` = "Others Segments" + "Soft Launch New Publishers BR" + "New Publishers BR" (from META)
- `Offsite + Instore` = Manual entries from Others tab with cat in ["Offsite", "Instore", "Instore + Offsite"]
- `Fees` = Manual entries from Others tab with cat in ["Performance Fee", "Fees", "Other Incomes"]
- `Long Tail` = Catch-all for publishers not mapped to other segments; meta is preserved (not recalculated)

### Others Tab Categories
Dropdown options: Instore + Offsite, Offsite, Instore, Performance Fee, Fees, Other Incomes

### AdTech vs AdNetwork Rule
- Advertiser name contains "vtexads" (case-insensitive) → **AdNetwork** (uses `trNetwork`)
- Otherwise → **AdTech** (uses `trTech`)

## Key Business Logic

### KPI Cards (renderKPIs)
- **Realizado** = realized publisher spend/rev + Others spend/rev
- **Forecast** = forecast model projection (linear/weighted/custom)
- **Projetado** = Realizado + Forecast + Others
- **MTD comparison** includes Others from previous month for fair baseline

### Segment Breakdown
- Regular segments: realized from REAL_APRIL publishers, forecast weighted by real spend proportion
- `Offsite + Instore`: spend + revenue from othersData filtered by instore categories
- `Fees`: spend + revenue from othersData filtered by fee categories
- Revenue computed dynamically: `spendTech × trTech + spendNetwork × trNetwork` per publisher

### Publisher Overrides (settings.json)
- `_applyOverrides()` moves publishers between segments at runtime
- `_recalcAfterOverrides()` recalculates segment totals (skips Long Tail to preserve its meta)
- Format: `{"PUBLISHER_NAME": {"seg": "Target Segment", "trTech": 0.1, "trNetwork": 0.15}}`

### Forecast Models
- Linear, Weighted, Custom spend per day
- `BLENDED_TR` = weighted average TR across all publishers

## Monthly Meta Source File (ARQUIVO FONTE DA META)
**⚠️ CRITICAL — NUNCA PERGUNTE AO USUÁRIO OS VALORES DA META. USE ESTE ARQUIVO.**

O arquivo fonte com as metas mensais por segmento e publisher é:
- **Arquivo:** `/sessions/pensive-zen-dirac/mnt/uploads/Untitled spreadsheet-6.xlsx`
- **Sheet:** `Sheet1`
- **Estrutura:** Linha 1 = headers dos meses (2026-01 até 2026-09+). Primeira seção = Ad Spend meta, segunda seção = Revenue meta.
- **Linhas principais:** Linha 3 = "Current Publishers BR" (total BR), depois breakdown por segmento com sub-linhas de publishers.
- **Mapeamento de colunas:** B=Jan, C=Fev, D=Mar, E=Abr, F=Mai, G=Jun, H=Jul, I=Ago, J=Set
- **Total geral:** Linha com nome vazio após todos os segmentos = META_SPEND_TOTAL / META_REV_TOTAL

### Processo de atualização mensal da meta
Sempre que o mês mudar (transição automática via update.py), atualizar:
1. Ler a coluna do novo mês neste arquivo
2. Atualizar `META_SPEND_TOTAL` e `META_REV_TOTAL` no index.html
3. Atualizar o objeto `META_APRIL` com os valores por segmento e publisher
4. Mapeamento de nomes: "Grocery" (planilha) → "Groceries" (dashboard)

### Histórico de metas (META_SPEND_TOTAL / META_REV_TOTAL)
- Jan: 9,415,241 / 1,358,997
- Fev: 11,597,866 / 1,672,283
- Mar: 16,712,634 / 2,654,555
- Abr: 16,211,039 / 2,558,171
- Mai: 18,642,352 / 3,366,953
- Jun: 22,947,743 / 3,727,052

## FX Rates (for LATAM)
- ARS → BRL: 0.0036
- COP → BRL: 0.0015
- PEN → BRL: 1.50

## Color Map
```
Beauty: #E85D75, Electronics: #3B82F6, Pharma: #F59E0B,
LATAM: #F97316, Long Tail: #6366F1, Home Center: #10B981,
Other Segments: #6B7785, Groceries: #06B6D4,
Offsite + Instore: #14B8A6, Fees: #8B5CF6
```

## Common Issues & Fixes
- **Git index.lock**: `rm -f .git/index.lock .git/HEAD.lock` (caused by concurrent git access from sandbox + terminal)
- **Push rejected**: `git pull --rebase origin main && git push origin main` (auto-commits from dashboard create remote commits)
- **Groceries 0 publishers**: Clear stale localStorage key `vtex_ads_forecast_settings`
- **Long Tail meta = 0**: The `_recalcAfterOverrides()` must skip Long Tail (`seg === 'Long Tail'` guard)
- **Python SSL error on Mac**: Use GitHub Actions instead (Python 3.9 on Mac has outdated SSL)

## Daily Gap Report (Slack)

### What it is
Daily report showing gap between realized ad spend vs R$ 18.6M monthly meta, with chart + text for Slack.

### Chart spec (VTEX Brand Guidelines)
- **Background:** white (#FFFFFF), generous whitespace
- **Primary accent:** Rebel Pink (#F71963) — realized line + realizado/gap KPIs
- **Text:** Serious Black (#142032) — titles, KPI values
- **Secondary:** Serious Gray (#5B6E84) labels, Cool Gray (#A1A8B7) subtexts, Winter Gray (#E7E9EE) dividers
- **Typography:** sans-serif, no bold titles, no all-caps, left-aligned, hierarchy by size (1.5 ratio)
- **Layout:** chart on left (58% width), KPI cards panel on right (realizado, pacing, projeção, gap, meta)
- **Lines:** realized (solid pink), meta (dashed gray), proj plataforma (dotted black), pacing (dash-dot cool gray)
- **Legend:** bottom, minimal
- Chart generation script: `/sessions/pensive-zen-dirac/gen_d25_final.py` (last working version)

### Text format
```
VTEX Ads · Gap report D{N}/{TD} — maio 2026

Realizado D1–D{N}: R$ {X}M (média R$ {Y}K/dia)
Pacing D{N}: R$ {Z}K/dia → R$ {W}M se mantiver
Proj. plataforma: R$ {P}M ({pct}% da meta)
Meta: R$ 18.6M
Gap: -R$ {G}M · precisa R$ {need}K/dia nos próximos {rem} dias

Top 5 advertisers AdNetwork — spend D{N} vs pico do mês:
1. {Name} — D{N}: R$ X/dia | Pico: R$ Y (D{peak}) | queda Z%
...
```

### Key files
- `scripts/daily_adv_network.py` — Fetches daily AdNetwork advertiser spend from Metabase, saves to `adv_network_daily.json`
- `.github/workflows/adv-network-daily.yml` — Manual dispatch to run the above
- `adv_network_daily.json` — Output: `{ "ADVERTISER_NAME": { "raw_name": "...", "total": N, "daily": { "2026-05-01": N, ... } } }`

### Data sources for the report
- `index.html` ACTUALS → daily total ad spend (D1–D{NA})
- `index.html` META_SPEND_TOTAL → R$ 18,642,352
- `index.html` ADV_DATA → advertiser monthly totals (but NO daily granularity)
- `index.html` REAL_DAILY_APR → daily spend per segment (NOT per advertiser)
- `adv_network_daily.json` → daily spend per AdNetwork advertiser (from Metabase)
- **Important:** ADV_DATA only stores monthly aggregates. For daily per-advertiser data, must use `adv_network_daily.json` (or query Metabase directly).

### Weighted projection model
```python
week_weights = {1: 0.18, 2: 0.20, 3: 0.22, 4: 0.40}  # S1=D1-7, S2=D8-14, S3=D15-21, S4=D22-31
proj = cumul_real / weight_so_far * weight_full_month
```

### Pending: Slack automation
User wants daily Slack message with chart + text until end of month. Options:
1. Scheduled GitHub Actions workflow that generates report + posts to Slack via webhook
2. Cowork scheduled task that reads data + sends via Slack MCP
To be implemented in a separate conversation to not interfere with dashboard development.

## Metabase Connection
- URL: metabase.newtail.com.br
- DB ID: 13 (Postgres)
- Card: 2368
- Auth: via `METABASE_USER` / `METABASE_PASS` env vars (stored as GitHub Secrets)
- Main table: `CAMPAIGNS_METRICS_NETWORK_DAY` joined with `publishers` and `advertisers`
