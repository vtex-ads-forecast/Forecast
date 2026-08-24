#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza a meta do MES VIGENTE no index.html com a meta oficial.

Fonte: planilha "Forecast + Meta Ads - 2026", abas "Meta - Forecast" e
"Meta - Forecast BASE" (snapshot 23/08/26).

O QUE MUDOU EM 23/08/26
-----------------------
Antes este script so atualizava META_SPEND_TOTAL e META_REV_TOTAL. O bloco
META_APRIL (meta por SEGMENTO e por PUBLISHER, que alimenta o Breakdown por
Segmento) nunca era tocado na virada do mes: ficou congelado em julho e o
dashboard passou agosto inteiro mostrando meta de JULHO no breakdown e meta de
AGOSTO no card do topo -- 2.474.363 de diferenca entre a soma das linhas e o
total exibido.

Agora o script vira o granular junto, e os TOTAIS sao DERIVADOS da soma dos
segmentos. Por construcao, o topo da tela sempre bate com a tabela.

O META_APRIL e JSON valido (chaves entre aspas), entao usamos json.loads em vez
de regex -- e a serializacao reproduz o layout original (1 segmento por linha,
publishers inline) para nao mexer nas ancoras do patch_ui.py.

FAIL-SAFE: qualquer anomalia => aviso e exit 0, sem tocar no arquivo.
"""
import json
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HTML_PATH = os.path.join(ROOT, "index.html")

# ---------------------------------------------------------------- meta oficial
# chave = nome do segmento COMO ESTA NO META_APRIL do app.
# valor = (ad spend, receita bruta)
META_SEG = {
    "2026-08": {
        "Electronics": (7442335, 780463), "Pharma": (7477413, 1387973),
        "Beauty": (682803, 56140), "Home Center": (278611, 26197),
        "Groceries": (95026, 21744), "LATAM": (1724219, 292551),
        "Long Tail": (2162497, 311400), "Others Segments": (5891, 560),
        "New Publishers BR": (15820, 4409), "Soft Launch New Publishers BR": (0, 0),
        "Instore + Offsite": (400000, 107500), "Performance Fee": (0, 77175),
        "Other Incomes": (0, 235000),
    },
    "2026-09": {
        "Electronics": (7905387, 838501), "Pharma": (10427908, 1848690),
        "Beauty": (748595, 61686), "Home Center": (297993, 28053),
        "Groceries": (114031, 26093), "LATAM": (1896641, 321806),
        "Long Tail": (2378747, 342540), "Others Segments": (6186, 588),
        "New Publishers BR": (17642, 4945), "Soft Launch New Publishers BR": (0, 0),
        "Instore + Offsite": (830000, 224000), "Performance Fee": (0, 81034),
        "Other Incomes": (0, 235000),
    },
    "2026-10": {
        "Electronics": (8418879, 904797), "Pharma": (12804571, 2262334),
        "Beauty": (822594, 67943), "Home Center": (318889, 30056),
        "Groceries": (136838, 31312), "LATAM": (2086305, 353986),
        "Long Tail": (2616621, 376793), "Others Segments": (6495, 617),
        "New Publishers BR": (19694, 5555), "Soft Launch New Publishers BR": (0, 0),
        "Instore + Offsite": (1344000, 365700), "Performance Fee": (0, 121551),
        "Other Incomes": (0, 235000),
    },
    "2026-11": {
        "Electronics": (22088681, 2499196), "Pharma": (20234122, 3610472),
        "Beauty": (1645768, 135972), "Home Center": (956667, 90168),
        "Groceries": (410513, 93935), "LATAM": (4172609, 707972),
        "Long Tail": (5233243, 753587), "Others Segments": (19485, 1852),
        "New Publishers BR": (22009, 6249), "Soft Launch New Publishers BR": (0, 0),
        "Instore + Offsite": (4782000, 1284600), "Performance Fee": (0, 364652),
        "Other Incomes": (0, 265000),
    },
    "2026-12": {
        "Electronics": (10739452, 1131236), "Pharma": (13296435, 2371342),
        "Beauty": (1069372, 88326), "Home Center": (414556, 39073),
        "Groceries": (177889, 40705), "LATAM": (2712196, 460182),
        "Long Tail": (3401608, 489832), "Others Segments": (8443, 802),
        "New Publishers BR": (24625, 7040), "Soft Launch New Publishers BR": (0, 0),
        "Instore + Offsite": (1747200, 475410), "Performance Fee": (0, 158016),
        "Other Incomes": (0, 235000),
    },
}

# grao de publisher (aba "Meta - Forecast BASE"): spend, spendTech, spendNetwork.
# publisher do app que NAO estiver aqui recebe o residuo do segmento, rateado
# pela proporcao que ja tinha -- a soma dos filhos sempre fecha com o pai.
PUB_META = {
    "2026-08": {
        "Electronics": {
            "CASAS BAHIA ADS": (4489709, 4464710, 24999),
            "KABUM": (1947691, 1918696, 28995),
            "BEMOL": (784112, 727483, 56629),
            "FAST SHOP": (107046, 84903, 22143),
            "AMERICANAS": (113493, 67290, 46203),
            "ELETRO ANGELONI": (284, 0, 284),
        },
        "Pharma": {
            "DROGARIA SÃO PAULO ADS": (4889603, 228642, 4660961),
            "PAGUE MENOS": (1422193, 175523, 1246670),
            "PANVEL": (1057742, 197935, 859807),
            "DROGAL ADS": (107875, 0, 107875),
        },
        "Beauty": {
            "SEPHORA ADS PUBLISHER": (616755, 543762, 72993),
            "ÉPOCA COSMÉTICOS ADS": (65522, 0, 65522),
            "AMOBELEZA": (526, 526, 0),
        },
        "Home Center": {
            "LEROY MERLIN": (109030, 109030, 0),
            "MADEIRA ADS": (169581, 169581, 0),
        },
        "Groceries": {
            "Super Nosso": (18208, 16, 18192),
            "PREZUNIC": (37973, 0, 37973),
            "ZONA SUL ADS": (14168, 0, 14168),
            "SUPER ANGELONI": (4082, 0, 4082),
            "SUPER MUFFATO DELIVERY": (4382, 0, 4382),
        },
        "LATAM": {
            "FARMACITY CONECT": (1724219, 1724219, 0),
        },
    },
}


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


def num(x):
    """int vira '7442335'; float vira '0.085' (sem zeros a toa)."""
    if isinstance(x, int):
        return str(x)
    s = repr(float(x))
    return s


def dump_meta_april(data):
    """serializa no MESMO layout do original: 1 segmento por linha, pubs inline."""
    linhas = []
    for seg, v in data.items():
        pubs = v.get("publishers")
        if pubs is None:
            linhas.append('  "%s": {"spendMeta": %d, "revMeta": %d}'
                          % (seg, v["spendMeta"], v["revMeta"]))
            continue
        inner = ", ".join(
            '"%s": {%s}' % (p, ", ".join('"%s": %s' % (k, num(pv)) for k, pv in d.items()))
            for p, d in pubs.items())
        linhas.append('  "%s": {"spendMeta": %d, "revMeta": %d, "publishers": {%s}}'
                      % (seg, v["spendMeta"], v["revMeta"], inner))
    return "{\n" + ",\n".join(linhas) + "\n}"


def aplica_segmento(v, sp, rv, oficial):
    """reescreve um segmento e seus publishers, garantindo que os filhos somem o pai."""
    v["spendMeta"], v["revMeta"] = sp, rv
    pubs = v.get("publishers")
    if not pubs:
        return

    conhecidos, resto = {}, []
    for nome, d in pubs.items():
        if nome in oficial:
            conhecidos[nome] = list(oficial[nome])
        else:
            resto.append(nome)

    residuo = sp - sum(x[0] for x in conhecidos.values())
    base = sum(pubs[n].get("spendMeta", 0) for n in resto)
    for nome in resto:
        d = pubs[nome]
        antigo = d.get("spendMeta", 0)
        quota = round(residuo * antigo / base) if base > 0 else (
            round(residuo / len(resto)) if resto else 0)
        t_old, n_old = d.get("spendTech", 0), d.get("spendNetwork", 0)
        tot = t_old + n_old
        t = round(quota * t_old / tot) if tot > 0 else quota
        conhecidos[nome] = [quota, t, quota - t]

    if conhecidos:
        dif = sp - sum(x[0] for x in conhecidos.values())
        if dif:
            maior = max(conhecidos, key=lambda k: conhecidos[k][0])
            conhecidos[maior][0] += dif
            conhecidos[maior][2] += dif

    # receita por publisher = tech*trTech + net*trNetwork, normalizada para
    # somar exatamente a receita oficial do segmento
    brutos = {}
    for nome, (s, t, n) in conhecidos.items():
        d = pubs[nome]
        brutos[nome] = t * d.get("trTech", 0) + n * d.get("trNetwork", 0)
    soma = sum(brutos.values())
    revs = {k: (round(rv * brutos[k] / soma) if soma > 0 else 0) for k in conhecidos}
    if revs:
        dif = rv - sum(revs.values())
        if dif:
            revs[max(revs, key=lambda k: revs[k])] += dif

    for nome, (s, t, n) in conhecidos.items():
        pubs[nome]["spendMeta"] = s
        pubs[nome]["revMeta"] = revs.get(nome, 0)
        pubs[nome]["spendTech"] = t
        pubs[nome]["spendNetwork"] = n


def main():
    if not os.path.exists(HTML_PATH):
        safe_exit("index.html nao encontrado")
    html = open(HTML_PATH, encoding="utf-8").read()

    m = re.search(r'"(\d{4}-\d{2})":\s*\{\s*[^}]*?status:\s*"current"', html)
    if not m:
        safe_exit("mes vigente nao encontrado em MONTHS_DATA")
    mkey = m.group(1)
    if mkey not in META_SEG:
        safe_exit(f"{mkey} sem meta oficial na tabela META_SEG")
    segs = META_SEG[mkey]
    pubs_of = PUB_META.get(mkey, {})

    sp_total = sum(v[0] for v in segs.values())
    rv_total = sum(v[1] for v in segs.values())

    b0, b1 = find_block(html, "META_APRIL")
    if b0 is None:
        safe_exit("bloco META_APRIL nao encontrado")
    try:
        data = json.loads(html[b0:b1])
    except Exception as e:
        safe_exit(f"META_APRIL nao e JSON valido: {e}")

    tocados = []
    for seg, (sp, rv) in segs.items():
        if seg not in data:
            print(f"[sync_meta] aviso: segmento '{seg}' nao existe no META_APRIL -- ignorado")
            continue
        aplica_segmento(data[seg], sp, rv, pubs_of.get(seg, {}))
        tocados.append(seg)

    if len(tocados) < 5:
        safe_exit(f"poucos segmentos reescritos ({len(tocados)}) -- abortando")

    # guarda: soma dos segmentos TEM que bater com o total, e cada segmento
    # TEM que bater com a soma dos seus publishers
    if sum(v["spendMeta"] for v in data.values()) != sp_total:
        safe_exit("soma dos segmentos != total de spend -- abortando")
    if sum(v["revMeta"] for v in data.values()) != rv_total:
        safe_exit("soma dos segmentos != total de receita -- abortando")
    for seg, v in data.items():
        pubs = v.get("publishers")
        if pubs and sum(p["spendMeta"] for p in pubs.values()) != v["spendMeta"]:
            safe_exit(f"'{seg}': soma dos publishers != o segmento -- abortando")

    html = html[:b0] + dump_meta_april(data) + html[b1:]

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
    print(f"[sync_meta] OK {mkey}: {len(tocados)} segmentos | "
          f"spend {sp_total:,} | receita {rv_total:,}")


if __name__ == "__main__":
    main()
