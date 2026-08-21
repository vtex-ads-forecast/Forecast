#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reconstrói o bloco REAL_APRIL (acumulado do mês por segmento/publisher) DO ZERO,
buscando o mês-a-data completo (dia 1 → ontem) no Metabase.

POR QUÊ: o update.py aplica DELTAS ao acumulado; re-processar dias com force=1
soma em dobro (visto 06/08: dia 2 duplicado → canais 2,15M vs diário 1,86M).
Reconstruir do zero todo dia elimina o erro e impede reincidência.

SEGURANÇA:
  - Usa as MESMAS funções do update.py (fetch, mapeamento, process_rows).
  - Preserva o segmento "Others" byte a byte (gerenciado manualmente).
  - Guardas: precisa achar o bloco, ≥3 segmentos e ≥20 publishers novos, e o
    total novo deve ficar entre 50% e 150% do antigo. Senão, NÃO altera nada.
  - FAIL-SAFE: qualquer exceção => exit 0 (job segue).
"""
import os
import re
import sys
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
HTML_PATH = os.path.join(ROOT, "index.html")


def safe_exit(msg):
    print(f"[rebuild_ra] {msg} — pulando sem erro")
    sys.exit(0)


def find_block(html):
    off = html.find("const REAL_APRIL=")
    if off == -1:
        off = html.find("const REAL_APRIL =")
    if off == -1:
        return None, None
    brace = html.find("{", off)
    d = 0
    for i in range(brace, len(html)):
        if html[i] == "{":
            d += 1
        elif html[i] == "}":
            d -= 1
            if d == 0:
                return brace, i + 1
    return None, None


def seg_block(block_text, seg):
    """extrai o sub-bloco '"Seg":{...}' (byte a byte) de dentro do REAL_APRIL"""
    key = f'"{seg}":'
    p = block_text.find(key)
    if p == -1:
        return None
    brace = block_text.find("{", p)
    d = 0
    for i in range(brace, len(block_text)):
        if block_text[i] == "{":
            d += 1
        elif block_text[i] == "}":
            d -= 1
            if d == 0:
                return block_text[p:i + 1]
    return None


def main():
    try:
        from update import metabase_auth, fetch_data, process_rows, extract_pub_mapping
    except Exception as e:
        safe_exit(f"import update.py falhou: {e}")

    if not os.path.exists(HTML_PATH):
        safe_exit("index.html não encontrado")
    html = open(HTML_PATH, encoding="utf-8").read()

    b0, b1 = find_block(html)
    if b0 is None:
        safe_exit("bloco REAL_APRIL não encontrado")
    old_block = html[b0:b1]

    # segmentos do bloco atual (para manter os mesmos, zerando os sem dado)
    old_segs = re.findall(r'"([^"]+)":\{spendReal:', old_block)
    if not old_segs:
        safe_exit("nenhum segmento no bloco atual")
    others_txt = seg_block(old_block, "Others")  # preservar verbatim

    ontem = date.today() - timedelta(days=1)
    first = ontem.replace(day=1)

    try:
        pub_seg, pub_tr = extract_pub_mapping(html)
        token = metabase_auth()
        # Busca DIA A DIA: a janela do mes inteiro estoura o teto de linhas do
        # Metabase. Visto em 21/08/26: 52.000 linhas no limite de seguranca e,
        # como o SQL ordena por "day DESC", os PRIMEIROS dias do mes eram
        # descartados -> acumulado ~20% menor que a serie diaria (ACTUALS).
        raw = []
        _d = first
        while _d <= ontem:
            raw.extend(fetch_data(token, _d.isoformat(), _d.isoformat()))
            _d += timedelta(days=1)
        data = process_rows(raw, pub_seg, pub_tr)
    except Exception as e:
        safe_exit(f"fetch/process falhou: {e}")

    segd, pubd = data["seg_delta"], data["pub_delta"]
    n_pubs = sum(len(v) for v in pubd.values())
    if len(segd) < 3 or n_pubs < 20:
        safe_exit(f"dados insuficientes (segs={len(segd)}, pubs={n_pubs})")

    # só segmentos de topo (têm ,publishers: logo após os 6 campos) — publishers não entram
    old_total = sum(int(x) for x in re.findall(
        r'"[^"]+":\{spendReal:(\d+),revReal:\d+,spendTech:\d+,spendNetwork:\d+,revTech:\d+,revNetwork:\d+,publishers:', old_block))
    new_total = round(sum(v["sp"] for v in segd.values()))
    if old_total > 0 and not (0.5 * old_total <= new_total <= 1.5 * old_total):
        safe_exit(f"total novo fora da faixa de segurança (novo={new_total}, antigo={old_total})")

    # GUARDA NOVA (21/08/26): o acumulado tem que bater com a serie diaria do
    # proprio index.html. A guarda de 50-150% compara com o valor ANTERIOR e por
    # isso nao enxerga EROSAO GRADUAL — foi assim que o mes derreteu ~20% sem
    # nenhum step do workflow falhar. Esta compara com uma fonte independente.
    am = re.search(r"const ACTUALS\s*=\s*\[(.*?)\]", html, re.S)
    if am:
        daily_total = sum(float(x) for x in re.findall(r"adspend:\s*([\d.]+)", am.group(1)))
        if daily_total > 0 and abs(new_total - daily_total) > 0.03 * daily_total:
            safe_exit(f"divergencia vs serie diaria: rebuild={new_total:,.0f} x ACTUALS={daily_total:,.0f}")

    def fmt_pub(name, v):
        tri = pub_tr.get(name, {"tech": 0.1, "net": 0.15})
        return ('"%s":{spendReal:%d,revReal:%d,spendTech:%d,spendNetwork:%d,revTech:%d,revNetwork:%d,trTech:%s,trNetwork:%s}'
                % (name, round(v["sp"]), round(v["rv"]), round(v["spT"]), round(v["spN"]),
                   round(v["rvT"]), round(v["rvN"]), round(tri.get("tech", 0.1), 4), round(tri.get("net", 0.15), 4)))

    parts = []
    ordered = [s for s in old_segs if s != "Others"] + [s for s in segd if s not in old_segs]
    seen = set()
    for seg in ordered:
        if seg in seen:
            continue
        seen.add(seg)
        v = segd.get(seg, {"sp": 0, "rv": 0, "spT": 0, "spN": 0, "rvT": 0, "rvN": 0})
        pubs = pubd.get(seg, {})
        pubs_txt = ",".join(fmt_pub(p, pv) for p, pv in sorted(pubs.items(), key=lambda x: -x[1]["sp"]))
        parts.append('"%s":{spendReal:%d,revReal:%d,spendTech:%d,spendNetwork:%d,revTech:%d,revNetwork:%d,publishers:{%s}}'
                     % (seg, round(v["sp"]), round(v["rv"]), round(v["spT"]), round(v["spN"]),
                        round(v["rvT"]), round(v["rvN"]), pubs_txt))
    if others_txt:
        parts.append(others_txt)

    new_block = "{" + ",\n".join(parts) + "}"
    out = html[:b0] + new_block + html[b1:]

    tmp = HTML_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(out)
    os.replace(tmp, HTML_PATH)
    print(f"[rebuild_ra] ✓ REAL_APRIL reconstruído {first} → {ontem}: {len(seen)} segs, {n_pubs} pubs | spend {old_total:,} → {new_total:,}")


if __name__ == "__main__":
    main()
