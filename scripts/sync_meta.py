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

FAIL-SAFE: qualquer anomalia => aviso e exit 0, sem tocar no arquivo.
"""
import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HTML_PATH = os.path.join(ROOT, "index.html")

# ---------------------------------------------------------------- meta oficial
# chave = nome do segmento COMO ESTA NO META_APRIL do app (nao como na planilha).
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
# publisher do app que NAO estiver aqui recebe o residuo do segmento rateado pela
# proporcao que ja tinha -- assim a soma dos filhos sempre fecha com o pai.
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
            "DROGARIA SAO PAULO ADS": (4889603, 228642, 4660961),
            "PAGUE MENOS": (1422193, 175523, 1246670),
            "PANVEL": (1057742, 197935, 859807),
            "DROGAL ADS": (107875, 0, 107875),
        },
        "Beauty": {
            "SEPHORA ADS PUBLISHER": (616755, 543762, 72993),
            "EPOCA COSMETICOS ADS": (65522, 0, 65522),
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

# acentos: o META_APRIL usa os nomes com acento; a tabela acima usa sem, para
# evitar problema de encoding. este de-para resolve.
ALIAS = {
    "DROGARIA SAO PAULO ADS": "DROGARIA SÃO PAULO ADS",
    "EPOCA COSMETICOS ADS": "ÉPOCA COSMÉTICOS ADS",
}


def safe_exit(msg):
    print(f"[sync_meta] {msg} -- pulando sem erro")
    sys.exit(0)


def find_block(html, name):
    off = html.find(f"const {name}=")
    if off == -1:
        off = html.find(f"const {name} =")
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


def seg_span(block, seg):
    """(inicio, fim) do sub-bloco de um segmento dentro do META_APRIL."""
    key = f'"{seg}":'
    p = block.find(key)
    if p == -1:
        return None
    brace = block.find("{", p)
    d = 0
    for i in range(brace, len(block)):
        if block[i] == "{":
            d += 1
        elif block[i] == "}":
            d -= 1
            if d == 0:
                return (p, i + 1)
    return None


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
    block = html[b0:b1]
    orig_block = block

    tocados = []
    for seg, (sp, rv) in segs.items():
        span = seg_span(block, seg)
        if span is None:
            print(f"[sync_meta] aviso: segmento '{seg}' nao existe no META_APRIL -- ignorado")
            continue
        s0, s1 = span
        sub = block[s0:s1]

        # --- publishers do segmento
        pub_rows = list(re.finditer(
            r'"([^"]+)":\{spendMeta:(\d+),revMeta:(\d+),spendTech:(\d+),spendNetwork:(\d+),'
            r'trTech:([\d.]+),trNetwork:([\d.]+)\}', sub))
        if pub_rows:
            oficial = {ALIAS.get(k, k): v for k, v in pubs_of.get(seg, {}).items()}
            novo = {}
            for pr in pub_rows:
                nome = pr.group(1)
                if nome in oficial:
                    novo[nome] = list(oficial[nome])
            resto = [pr for pr in pub_rows if pr.group(1) not in novo]
            residuo = sp - sum(v[0] for v in novo.values())
            base = sum(int(pr.group(2)) for pr in resto)
            for pr in resto:
                nome = pr.group(1)
                antigo_sp = int(pr.group(2))
                quota = round(residuo * (antigo_sp / base)) if base > 0 else (
                    round(residuo / len(resto)) if resto else 0)
                antigo_t, antigo_n = int(pr.group(4)), int(pr.group(5))
                tot = antigo_t + antigo_n
                t = round(quota * (antigo_t / tot)) if tot > 0 else quota
                novo[nome] = [quota, t, quota - t]
            # ajuste de centavo no maior, para fechar exatamente com o segmento
            if novo:
                dif = sp - sum(v[0] for v in novo.values())
                if dif:
                    maior = max(novo, key=lambda k: novo[k][0])
                    novo[maior][0] += dif
                    novo[maior][2] += dif

            # receita por publisher = tech*trTech + net*trNetwork, normalizada
            # para somar exatamente a receita oficial do segmento
            tr = {pr.group(1): (float(pr.group(6)), float(pr.group(7))) for pr in pub_rows}
            brutos = {k: novo[k][1] * tr[k][0] + novo[k][2] * tr[k][1] for k in novo}
            soma = sum(brutos.values())
            revs = {}
            for k in novo:
                revs[k] = round(rv * (brutos[k] / soma)) if soma > 0 else 0
            if revs:
                dif = rv - sum(revs.values())
                if dif:
                    revs[max(revs, key=lambda k: revs[k])] += dif

            def repl(mo):
                nome = mo.group(1)
                if nome not in novo:
                    return mo.group(0)
                s, t, n = novo[nome]
                return ('"%s":{spendMeta:%d,revMeta:%d,spendTech:%d,spendNetwork:%d,'
                        'trTech:%s,trNetwork:%s}' % (nome, s, revs.get(nome, 0), t, n,
                                                     mo.group(6), mo.group(7)))

            sub = re.sub(
                r'"([^"]+)":\{spendMeta:(\d+),revMeta:(\d+),spendTech:(\d+),spendNetwork:(\d+),'
                r'trTech:([\d.]+),trNetwork:([\d.]+)\}', repl, sub)

        # --- o proprio segmento
        sub_novo, n = re.subn(r'^("' + re.escape(seg) + r'":\{)spendMeta:\d+,revMeta:\d+',
                              r'\g<1>spendMeta:%d,revMeta:%d' % (sp, rv), sub, count=1)
        if n != 1:
            safe_exit(f"nao consegui reescrever o cabecalho do segmento '{seg}'")
        block = block[:s0] + sub_novo + block[s1:]
        tocados.append(seg)

    if len(tocados) < 5:
        safe_exit(f"poucos segmentos reescritos ({len(tocados)}) -- abortando")

    # --- guarda final: a soma dos segmentos TEM que bater com o total
    somas = [int(x) for x in re.findall(
        r'"[^"]+":\{spendMeta:(\d+),revMeta:\d+,publishers:', block)]
    if somas and sum(somas) != sp_total:
        safe_exit(f"soma dos segmentos ({sum(somas):,}) != total ({sp_total:,}) -- abortando")

    html = html[:b0] + block + html[b1:]

    # --- totais derivados da propria tabela (nunca mais divergem do breakdown)
    html, n1 = re.subn(r"const META_SPEND_TOTAL\s*=\s*\d+",
                       f"const META_SPEND_TOTAL = {sp_total}", html, count=1)
    html, n2 = re.subn(r"const META_REV_TOTAL\s*=\s*\d+",
                       f"const META_REV_TOTAL = {rv_total}", html, count=1)
    if not (n1 and n2):
        safe_exit("constantes META_*_TOTAL nao encontradas")

    if block == orig_block and n1 and n2:
        print(f"[sync_meta] META_APRIL de {mkey} ja estava sincronizado")

    tmp = HTML_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(html)
    os.replace(tmp, HTML_PATH)
    print(f"[sync_meta] OK {mkey}: {len(tocados)} segmentos | "
          f"spend {sp_total:,} | receita {rv_total:,}")


if __name__ == "__main__":
    main()
