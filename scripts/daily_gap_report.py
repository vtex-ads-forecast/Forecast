#!/usr/bin/env python3
"""
VTEX Ads — Daily Gap Report
Gera gráfico (matplotlib, brand VTEX) + texto do gap report e posta no Slack.

Usage:
  python scripts/daily_gap_report.py

Environment variables:
  SLACK_BOT_TOKEN   — Slack bot token (xoxb-...)
  SLACK_CHANNEL_ID  — Slack channel ID (ex: C01234ABCDE)
"""

import os, json, re, io
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
INDEX_HTML = ROOT / "index.html"
ADV_NETWORK_JSON = ROOT / "adv_network_daily.json"

# ── Business constants ─────────────────────────────────────────────────────
MONTH_NAME   = "maio 2026"
MONTH_PREFIX = "2026-05"
MONTH_DAYS   = 31

# ── VTEX Brand colors ──────────────────────────────────────────────────────
REBEL_PINK    = "#F71963"
SERIOUS_BLACK = "#142032"
SERIOUS_GRAY  = "#5B6E84"
COOL_GRAY     = "#A1A8B7"
WINTER_GRAY   = "#E7E9EE"


# ── Parsing ────────────────────────────────────────────────────────────────

def parse_js_var(html: str, name: str):
    """Extract a JS variable (number or JSON array/object) from index.html."""
    # Match: const NAME = <value>;
    m = re.search(rf"const {re.escape(name)}\s*=\s*([^\n;]+);", html)
    if not m:
        raise ValueError(f"{name} not found in index.html")
    raw = m.group(1).strip()
    return json.loads(raw)


def load_html_data():
    html = INDEX_HTML.read_text(encoding="utf-8")
    actuals = parse_js_var(html, "ACTUALS")       # [{day, adspend}]
    na      = parse_js_var(html, "NA")            # int
    td      = parse_js_var(html, "TD")            # int
    meta    = parse_js_var(html, "META_SPEND_TOTAL")  # float
    return actuals, int(na), int(td), float(meta)


# ── Projection model ───────────────────────────────────────────────────────

WEEK_W    = {1: 0.18, 2: 0.20, 3: 0.22, 4: 0.40}
WEEK_DAYS = {1: 7,    2: 7,    3: 7,    4: 10}


def day_weight(d: int) -> float:
    if   d <=  7: wk = 1
    elif d <= 14: wk = 2
    elif d <= 21: wk = 3
    else:         wk = 4
    return WEEK_W[wk] / WEEK_DAYS[wk]


def weighted_projection(actuals: list, na: int) -> float:
    """Projeção ponderada por semana: cumul / weight_so_far * 1.0"""
    cumul          = sum(a["adspend"] for a in actuals if a["day"] <= na)
    weight_so_far  = sum(day_weight(d) for d in range(1, na + 1))
    if weight_so_far == 0:
        return 0.0
    return cumul / weight_so_far   # weight_full = 1.0


# ── Chart ──────────────────────────────────────────────────────────────────

def generate_chart(actuals: list, na: int, td: int, meta_total: float, proj: float) -> bytes:
    """Gera o gráfico do gap report e retorna bytes PNG."""

    # Cumulative realized
    daily_map = {a["day"]: a["adspend"] for a in actuals}
    cumul_real = {}
    running = 0.0
    for d in range(1, na + 1):
        running += daily_map.get(d, 0)
        cumul_real[d] = running
    last_real = cumul_real.get(na, 0)

    days_all = list(range(1, td + 1))

    # Meta line (linear)
    meta_line = [meta_total * d / td for d in days_all]

    # Pacing (linear from current rate)
    pacing_daily = last_real / na if na > 0 else 0
    pacing_line  = [pacing_daily * d for d in days_all]

    # Projection line: actual until NA, then straight to proj at td
    proj_line = []
    for d in days_all:
        if d <= na:
            proj_line.append(cumul_real.get(d, 0))
        else:
            frac = (d - na) / (td - na) if td > na else 1
            proj_line.append(last_real + frac * (proj - last_real))

    # ── Figure layout ──────────────────────────────────────────────────────
    fig = plt.figure(figsize=(15, 7), facecolor="white", dpi=130)

    # Chart axes: left 57%
    ax = fig.add_axes([0.05, 0.13, 0.53, 0.75])

    # Meta (dashed, cool gray)
    ax.plot(days_all, meta_line, "--", color=COOL_GRAY, linewidth=1.5, zorder=2)
    # Pacing (dash-dot, serious gray)
    ax.plot(days_all, pacing_line, "-.", color=SERIOUS_GRAY, linewidth=1.2, zorder=2)
    # Projection plataforma (dotted, serious black)
    ax.plot(days_all, proj_line, ":", color=SERIOUS_BLACK, linewidth=1.8, zorder=3)
    # Realized (solid, rebel pink) — on top
    ax.plot(list(cumul_real.keys()), list(cumul_real.values()),
            "-", color=REBEL_PINK, linewidth=2.8, zorder=5)
    ax.plot(na, last_real, "o", color=REBEL_PINK, markersize=6, zorder=6)

    # Today divider
    ax.axvline(x=na + 0.5, color=WINTER_GRAY, linewidth=1, zorder=1)

    # Axes formatting
    y_max = max(meta_total, proj, pacing_daily * td) * 1.08
    ax.set_xlim(1, td)
    ax.set_ylim(0, y_max)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f"R${x/1e6:.1f}M"))
    ax.set_xlabel("Dia", color=SERIOUS_GRAY, fontsize=9, labelpad=4)
    ax.tick_params(colors=SERIOUS_GRAY, labelsize=8.5)
    for spine in ax.spines.values():
        spine.set_edgecolor(WINTER_GRAY)
    ax.grid(axis="y", color=WINTER_GRAY, linewidth=0.6, zorder=0)

    # Title
    fig.text(0.05, 0.955,
             f"VTEX Ads · Gap report D{na}/{td} — {MONTH_NAME}",
             color=SERIOUS_BLACK, fontsize=13, va="top", fontfamily="sans-serif")

    # Legend
    legend_handles = [
        Line2D([0], [0], color=REBEL_PINK,    linewidth=2.5,              label="Realizado"),
        Line2D([0], [0], color=SERIOUS_BLACK,  linestyle=":",  linewidth=1.8, label="Proj. plataforma"),
        Line2D([0], [0], color=SERIOUS_GRAY,   linestyle="-.", linewidth=1.2, label="Pacing"),
        Line2D([0], [0], color=COOL_GRAY,      linestyle="--", linewidth=1.5, label="Meta"),
    ]
    ax.legend(handles=legend_handles, loc="upper left", fontsize=8.5,
              frameon=False, labelcolor=SERIOUS_GRAY, ncol=2)

    # ── KPI panel (right 38%) ──────────────────────────────────────────────
    px = 0.63   # panel left edge (figure coords)
    pw = 0.33   # panel width

    avg_daily   = last_real / na if na > 0 else 0
    pacing_eom  = pacing_daily * td
    gap         = proj - meta_total
    rem_days    = td - na
    need_daily  = (meta_total - last_real) / rem_days if rem_days > 0 else 0
    pct         = proj / meta_total * 100 if meta_total else 0

    gap_color = REBEL_PINK if gap < 0 else "#10B981"

    def fmt_m(v):  return f"R$ {v/1e6:.2f}M"
    def fmt_k(v):  return f"R$ {v/1e3:.0f}K"

    kpis = [
        ("Realizado",         fmt_m(last_real),  f"D1–D{na} · média {fmt_k(avg_daily)}/dia", REBEL_PINK),
        ("Pacing",            fmt_m(pacing_eom), f"{fmt_k(pacing_daily)}/dia atual",          SERIOUS_GRAY),
        ("Proj. plataforma",  fmt_m(proj),        f"{pct:.0f}% da meta",                       SERIOUS_BLACK),
        ("Gap",               f"{'-' if gap<0 else '+'}R$ {abs(gap)/1e6:.2f}M",
                              f"precisa {fmt_k(need_daily)}/dia · {rem_days}d restantes",     gap_color),
        ("Meta",              fmt_m(meta_total),  MONTH_NAME,                                  COOL_GRAY),
    ]

    n_cards   = len(kpis)
    card_h    = 0.14
    card_gap  = 0.025
    start_y   = 0.88

    for i, (label, value, sub, color) in enumerate(kpis):
        y = start_y - i * (card_h + card_gap)
        # Horizontal rule
        line = plt.Line2D([px, px + pw], [y + 0.005, y + 0.005],
                          transform=fig.transFigure,
                          color=WINTER_GRAY, linewidth=0.8)
        fig.add_artist(line)
        fig.text(px, y - 0.008,  label, color=COOL_GRAY,     fontsize=8.5,  va="top")
        fig.text(px, y - 0.044,  value, color=color,         fontsize=15.5, va="top")
        fig.text(px, y - 0.085,  sub,   color=COOL_GRAY,     fontsize=8,    va="top")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── Text report ────────────────────────────────────────────────────────────

def format_text(actuals: list, na: int, td: int, meta_total: float,
                proj: float, adv_data: dict) -> str:
    last_real  = sum(a["adspend"] for a in actuals if a["day"] <= na)
    avg_daily  = last_real / na if na > 0 else 0
    pacing_d   = last_real / na if na > 0 else 0
    pacing_eom = pacing_d * td
    gap        = proj - meta_total
    rem_days   = td - na
    need_daily = (meta_total - last_real) / rem_days if rem_days > 0 else 0
    pct        = proj / meta_total * 100 if meta_total else 0

    lines = [
        f"*VTEX Ads · Gap report D{na}/{td} — {MONTH_NAME}*",
        "",
        f"Realizado D1–D{na}: R$ {last_real/1e6:.2f}M (média R$ {avg_daily/1e3:.0f}K/dia)",
        f"Pacing D{na}: R$ {pacing_d/1e3:.0f}K/dia → R$ {pacing_eom/1e6:.2f}M se mantiver",
        f"Proj. plataforma: R$ {proj/1e6:.2f}M ({pct:.0f}% da meta)",
        f"Meta: R$ {meta_total/1e6:.1f}M",
        f"Gap: {'-' if gap<0 else '+'}R$ {abs(gap)/1e6:.2f}M · precisa R$ {need_daily/1e3:.0f}K/dia "
        f"nos próximos {rem_days} dias",
    ]

    # Top 5 oportunidades AdNetwork: maior gap absoluto entre pico do mês e D{na}
    if adv_data:
        today_key = f"{MONTH_PREFIX}-{na:02d}"
        top = []
        for name, info in adv_data.items():
            days = info.get("daily", {})
            if not days:
                continue
            peak_key  = max(days, key=days.get)
            peak_val  = days[peak_key]
            peak_d    = int(peak_key.split("-")[2])
            today_s   = days.get(today_key, 0)
            gap_adv   = peak_val - today_s          # oportunidade = quanto está abaixo do pico
            pct_queda = (1 - today_s / peak_val) * 100 if peak_val > 0 else 0
            if peak_val < 1_000:                    # ignora advertisers com pico irrelevante (<R$1K)
                continue
            top.append((name, today_s, peak_val, peak_d, gap_adv, pct_queda))

        # Ordenar pelo maior gap absoluto (pico - D{na})
        top.sort(key=lambda x: -x[4])
        top5 = top[:5]

        if top5:
            lines += [
                "",
                f"*Top 5 oportunidades AdNetwork — gap pico vs D{na}:*",
            ]
            for i, (name, ts, pv, pd, gap_adv, pct) in enumerate(top5, 1):
                lines.append(
                    f"{i}. {name} — Pico: R$ {pv/1e3:.0f}K (D{pd}) | "
                    f"D{na}: R$ {ts/1e3:.0f}K | gap -R$ {gap_adv/1e3:.0f}K/dia ({pct:.0f}% abaixo)"
                )

    return "\n".join(lines)


# ── Output ─────────────────────────────────────────────────────────────────

CHART_PNG  = ROOT / "gap_report_latest.png"
REPORT_JSON = ROOT / "gap_report_latest.json"

GITHUB_PAGES_BASE = "https://vtex-ads-forecast.github.io/Forecast"


def save_outputs(chart_png: bytes, text: str, na: int, td: int):
    """Salva gráfico e JSON no repo para serem servidos pelo GitHub Pages."""
    from datetime import datetime, timezone

    # Salvar imagem
    CHART_PNG.write_bytes(chart_png)
    print(f"✓ Gráfico salvo em {CHART_PNG}")

    # Salvar JSON com texto e metadados para a Cowork scheduled task
    data = {
        "text": text,
        "chart_url": f"{GITHUB_PAGES_BASE}/gap_report_latest.png",
        "na": na,
        "td": td,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    REPORT_JSON.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✓ JSON salvo em {REPORT_JSON}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    # Carregar dados do index.html
    actuals, na, td, meta_total = load_html_data()
    print(f"✓ ACTUALS: {len(actuals)} dias | NA={na} | TD={td} | Meta=R${meta_total:,.0f}")

    if na == 0:
        print("⚠️  NA=0 — nenhum dado ainda, abortando.")
        return

    # Carregar dados AdNetwork
    adv_data = {}
    if ADV_NETWORK_JSON.exists():
        adv_data = json.loads(ADV_NETWORK_JSON.read_text(encoding="utf-8"))
        print(f"✓ AdNetwork: {len(adv_data)} advertisers")
    else:
        print("⚠️  adv_network_daily.json não encontrado — Top 5 será omitido")

    # Projeção ponderada
    proj = weighted_projection(actuals, na)
    print(f"✓ Proj. plataforma: R$ {proj/1e6:.2f}M ({proj/meta_total*100:.1f}% da meta)")

    # Gerar gráfico
    chart_png = generate_chart(actuals, na, td, meta_total, proj)
    print(f"✓ Gráfico gerado ({len(chart_png):,} bytes)")

    # Formatar texto
    text = format_text(actuals, na, td, meta_total, proj, adv_data)
    print("✓ Texto:\n" + text)

    # Salvar arquivos no repo (GitHub Actions fará o commit)
    save_outputs(chart_png, text, na, td)
    print("✓ Concluído!")


if __name__ == "__main__":
    main()
