#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza a meta do MES VIGENTE no index.html.

O QUE ESTE SCRIPT FAZ
---------------------
1. Copia META_FUTURE[<mes vigente>] para dentro de META_APRIL (meta por
   SEGMENTO e por PUBLISHER -- e o que alimenta o Breakdown por Segmento).
2. Deriva META_SPEND_TOTAL / META_REV_TOTAL da SOMA dos segmentos.

POR QUE (bug de 21-23/08/26)
----------------------------
Antes, este script so mexia nas duas constantes de total. O META_APRIL nunca
era tocado na virada do mes -- ficou congelado em julho. Resultado: durante todo
agosto o dashboard mostrou meta de JULHO no breakdown (17.810.252) e meta de
AGOSTO no card do topo (20.284.615). 2.474.363 de diferenca entre a soma das
linhas e o total, e todo GAP por segmento subestimado (Pharma aparecia -1,84M
quando o real era -3,15M).

A meta granular correta JA ESTAVA no repo, em META_FUTURE (ago-dez/26, com
grao de publisher, conferida contra a aba "Meta - Forecast BASE" da planilha
"Forecast + Meta Ads - 2026"). O que faltava era ligar os dois blocos.

Derivar os totais da soma e o que garante que o topo da tela e a tabela nunca
mais divirjam: nao existe mais um lugar onde um possa mudar sem o outro.

NOTA: os blocos sao literais JS, nao JSON -- podem ter virgula sobrando antes de
'}'. Por isso o load() limpa antes de dar json.loads.

FAIL-SAFE: qualquer anomalia => aviso e exit 0, sem tocar no arquivo.
"""
import json
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HTML_PATH = os.path.join(ROOT, "index.html")

TOLERANCIA = 5  # reais de arredondamento aceitos por segmento


def safe_exit(msg):
    print(f"[sync_meta] {msg} -- pulando sem erro")
    sys.exit(0)


def find_block(html, name):
    """(inicio, fim) do literal de objeto de 'const <name> = {...}'."""
    m = re.search(r"const\s+" + name + r"\s*=\s*\{", html)
    if not m:
        return None, None
    brace = html.index("{", m.start())
    d = 0
    for i in range(brace, len(html)):
        if html[i] == "{":
            d += 1
        elif html[i] == "}":
            d -= 1
            if d == 0:
                return brace, i + 1
    return None, None


def load(txt):
    """literal JS -> dict (tolera virgula sobrando)."""
    return json.loads(re.sub(r",(\s*[}\]])", r"\1", txt))


def num(x):
    return str(x) if isinstance(x, int) else repr(float(x))


def dump_meta_april(data):
    """serializa no MESMO layout do original: 1 segmento por linha, pubs inline."""
    linhas = []
    for seg, v in data.items():
        campos = '"spendMeta": %d, "revMeta": %d' % (v["spendMeta"], v["revMeta"])
        pubs = v.get("publishers")
        if pubs is None:
            linhas.append('  "%s": {%s}' % (seg, campos))
            continue
        inner = ", ".join(
            '"%s": {%s}' % (p, ", ".join('"%s": %s' % (k, num(pv)) for k, pv in d.items()))
            for p, d in pubs.items())
        linhas.append('  "%s": {%s, "publishers": {%s}}' % (seg, campos, inner))
    return "{\n" + ",\n".join(linhas) + "\n}"


def main():
    if not os.path.exists(HTML_PATH):
        safe_exit("index.html nao encontrado")
    html = open(HTML_PATH, encoding="utf-8").read()

    m = re.search(r'"(\d{4}-\d{2})":\s*\{\s*[^}]*?status:\s*"current"', html)
    if not m:
        safe_exit("mes vigente nao encontrado em MONTHS_DATA")
    mkey = m.group(1)

    f0, f1 = find_block(html, "META_FUTURE")
    b0, b1 = find_block(html, "META_APRIL")
    if f0 is None:
        safe_exit("bloco META_FUTURE nao encontrado")
    if b0 is None:
        safe_exit("bloco META_APRIL nao encontrado")

    try:
        futuro = load(html[f0:f1])
        atual = load(html[b0:b1])
    except Exception as e:
        safe_exit(f"bloco de meta nao e JSON valido: {e}")

    if mkey not in futuro:
        safe_exit(f"{mkey} nao existe em META_FUTURE -- atualizar o bloco no index.html")
    novo = futuro[mkey]

    # ---- guardas antes de escrever
    if len(novo) < 5:
        safe_exit(f"META_FUTURE[{mkey}] tem so {len(novo)} segmentos -- abortando")
    for seg, v in novo.items():
        if "spendMeta" not in v or "revMeta" not in v:
            safe_exit(f"'{seg}' sem spendMeta/revMeta -- abortando")
        pubs = v.get("publishers")
        if pubs:
            soma = sum(p.get("spendMeta", 0) for p in pubs.values())
            if abs(soma - v["spendMeta"]) > TOLERANCIA:
                safe_exit(f"'{seg}': publishers somam {soma:,} mas o segmento diz "
                          f"{v['spendMeta']:,} -- abortando")

    sp_total = sum(v["spendMeta"] for v in novo.values())
    rv_total = sum(v["revMeta"] for v in novo.values())
    sp_antes = sum(v.get("spendMeta", 0) for v in atual.values())

    if not (0.3 * sp_antes <= sp_total <= 3.0 * sp_antes) and sp_antes > 0:
        safe_exit(f"meta nova ({sp_total:,}) fora da faixa de seguranca vs a atual "
                  f"({sp_antes:,}) -- abortando")

    if atual == novo:
        print(f"[sync_meta] META_APRIL de {mkey} ja estava sincronizado")
    else:
        html = html[:b0] + dump_meta_april(novo) + html[b1:]

    html, n1 = re.subn(r"const META_SPEND_TOTAL\s*=\s*\d+",
                       f"const META_SPEND_TOTAL = {sp_total}", html, count=1)
    html, n2 = re.subn(r"const META_REV_TOTAL\s*=\s*\d+",
                       f"const META_REV_TOTAL = {rv_total}", html, count=1)
    if not (n1 and n2):
        safe_exit("constantes META_*_TOTAL nao encontradas")

    tmp = HTML_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(html)
    os.replace(tmp, HTML_PATH)
    print(f"[sync_meta] OK {mkey}: {len(novo)} segmentos | spend {sp_antes:,} -> "
          f"{sp_total:,} | receita {rv_total:,}")


if __name__ == "__main__":
    main()
