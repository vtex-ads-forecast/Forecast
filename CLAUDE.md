# VTEX Ads Forecast Dashboard — Project Context
## ⚠️ FONTE OFICIAL DE REGRAS E TAKE RATES (adicionado em 2026-07-21)

Este dashboard CONSOME regras de negócio cuja fonte da verdade vive FORA deste repo:

- **Take rates (settings.json): NÃO EDITAR NA MÃO.** O arquivo é GERADO a partir da aba
  `takerate` do Fechamento Simplificado (Google Drive · pasta "Finance VTEX Ads" do João).
  Mudou um acordo → atualiza-se a aba takerate → o settings.json é regerado pelo Claude
  na sessão Cowork do projeto Finance. Exceções conscientes (proxies) têm campo "nota".
- **Regras de negócio** (AdTech×AdNetwork, escadas progressivas, institucionais, publishers
  de teste, vigências temporárias como Panvel 73% jul-set/26, bonificadas, teto de PI):
  documentadas no vault Obsidian `Finance VTEX Ads/Documentação/` (fechamento-regras-de-negocio.md,
  fechamento-take-rates.md, forecast-divergencias-settings.md).
- **Receita realizada oficial** = fechamento mensal (charge/comissão/repasse), NÃO spend×TR.
  Calibração mensal via historico_fechamentos.json (a ser gerado do fechamento oficial).
- Proxies conscientes ativos: KABUM trTech 7% (aproxima receita Ad Request) — decisão João 21/07.

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

## Monthly Meta Source File — ATUALIZADO em 2026-07-21 (NOVA META OFICIAL Jun–Dez)

⚠️ O arquivo antigo ("Untitled spreadsheet-6.xlsx") está OBSOLETO. A fonte oficial da meta agora é:
- **Planilha:** "Forecast + Meta Ads - 2026" (Google Sheets, João) — docs.google.com/spreadsheets/d/1vYZsrJvQ9OFRN_xSaIiBY5tboJG6-wI2dFNo_kE_9xA
- **Aba oficial:** "Meta - Forecast" (as abas "Visão As-Is" e "Copy of" NÃO valem)
- **Estrutura:** seção AD SPEND (linhas ~6–115) e seção RECEITA (linhas ~118–173), hierarquia Segmento > Publisher > AdTech/AdNetwork; colunas Jun..Dez. Meta de receita do dashboard = RECEITA BRUTA.
- **Snapshot granular:** meta_oficial_2026_snapshot.json (neste repo) + doc no vault Finance (hub-finance-ads/meta-oficial-2026.md)
- Mapeamento de segmentos: "Novos Publishers"→New Publishers BR · "Other Segments (legado)"→Others Segments · Offsite+Instore→"Instore + Offsite" · Performance Fee e Other Incomes → segmentos próprios · Kabum receita = proxy flat 150k/mês (300k nov).

### Histórico de metas NOVO (META_SPEND_TOTAL / META_REV_TOTAL — receita bruta)
- Jan: 9.415.241 / 1.358.997 · Fev: 11.597.866 / 1.672.283 · Mar: 16.712.634 / 2.654.555 · Abr: 16.211.039 / 2.558.171 · Mai: 18.642.352 / 3.366.953 (meta antiga, meses já comunicados)
- **Jun: 15.880.868 / 2.297.568 (RETROATIVO — decisão João 21/07)**
- Jul: 17.810.252 / 2.806.618 · Ago: 20.284.614 / 3.301.112 · Set: 24.623.129 / 4.012.935
- Out: 28.574.887 / 4.755.644 · Nov: 59.565.096 / 9.813.656 · Dez: 33.591.776 / 5.496.963

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
Daily report showing gap between realized ad spend vs monthly meta (META_SPEND_TOTAL vigente — jul/26: R$ 17.8M), with chart + text for Slack.

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
