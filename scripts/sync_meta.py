#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza META_SPEND_TOTAL / META_REV_TOTAL do index.html com a meta oficial
do MÊS VIGENTE (o close mensal do update.py vira o mês, mas não atualiza as constantes).

Fonte da tabela: planilha oficial "Forecast + Meta Ads - 2026", aba "Meta - Forecast"
(snapshot 21/07 — se a meta oficial mudar, atualizar a tabela abaixo).

FAIL-SAFE: qualquer anomalia => imprime aviso e sai com código 0, sem tocar no arquivo.
"""
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HTML_PATH = os.path.join(ROOT, "index.html")

# meta oficial TOTAL por mês (spend, receita) — jun-dez/2026
META_OFICIAL = {
    "2026-06": (15880868, 2297568),
    "2026-07": (17810252, 2806618),
    "2026-08": (20284615, 3301110),
    "2026-09": (24623130, 4012936),
    "2026-10": (28574886, 4755643),
    "2026-11": (59565094, 9813657),
    "2026-12": (33591776, 5496965),
}


def safe_exit(msg):
    print(f"[sync_meta] {msg} — pulando sem erro")
    sys.exit(0)


def main():
    if not os.path.exists(HTML_PATH):
        safe_exit("index.html não encontrado")
    html = open(HTML_PATH, encoding="utf-8").read()

    # mês vigente = entrada de MONTHS_DATA com status "current"
    m = re.search(r'"(\d{4}-\d{2})":\s*\{\s*[^}]*?status:\s*"current"', html)
    if not m:
        safe_exit("mês vigente não encontrado em MONTHS_DATA")
    mkey = m.group(1)
    if mkey not in META_OFICIAL:
        safe_exit(f"{mkey} sem meta oficial na tabela")
    sp, rv = META_OFICIAL[mkey]

    sp_m = re.search(r"const META_SPEND_TOTAL\s*=\s*(\d+)", html)
    rv_m = re.search(r"const META_REV_TOTAL\s*=\s*(\d+)", html)
    if not sp_m or not rv_m:
        safe_exit("constantes META_*_TOTAL não encontradas")

    if int(sp_m.group(1)) == sp and int(rv_m.group(1)) == rv:
        print(f"[sync_meta] ✓ meta de {mkey} já sincronizada ({sp:,} / {rv:,})")
        return

    html = re.sub(r"const META_SPEND_TOTAL\s*=\s*\d+",
                  f"const META_SPEND_TOTAL = {sp}", html, count=1)
    html = re.sub(r"const META_REV_TOTAL\s*=\s*\d+",
                  f"const META_REV_TOTAL = {rv}", html, count=1)

    tmp = HTML_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(html)
    os.replace(tmp, HTML_PATH)
    print(f"[sync_meta] ✓ {mkey}: META_SPEND_TOTAL {sp_m.group(1)} → {sp} | META_REV_TOTAL {rv_m.group(1)} → {rv}")


if __name__ == "__main__":
    main()
