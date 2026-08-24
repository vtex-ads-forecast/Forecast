#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Atualiza o abas-data.json (abas AdNetwork / AdTech do dashboard) com o MÊS VIGENTE,
por advertiser × publisher, usando o MESMO feed Metabase do update.py.

REGRAS DE SEGURANÇA (não quebrar nada):
  - 100%% aditivo: NÃO toca no index.html nem em nenhum outro arquivo.
  - Nunca sobrescreve meses fechados (lista _meses_fechados do próprio JSON).
  - Qualquer erro => imprime aviso e sai com código 0 (o job do Actions continua).
  - Se abas-data.json não existir, não faz nada.

Canal (mesma heurística do update.py, linha ~402):
  - 'vtexads' no advertiser_name  => AdNetwork (indústria)
  - caso contrário                => AdTech (inclui sellers — decisão João 28/07)
Receita = cost_brl × TR do settings.json (trNetwork / trTech por publisher).
"""
import json
import os
import re
import sys
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.join(HERE, "..")
ABAS_PATH = os.path.join(ROOT, "abas-data.json")
SETTINGS_PATH = os.path.join(ROOT, "settings.json")

MES_LBL = {1: "jan", 2: "fev", 3: "mar", 4: "abr", 5: "mai", 6: "jun",
           7: "jul", 8: "ago", 9: "set", 10: "out", 11: "nov", 12: "dez"}


def safe_exit(msg):
    print(f"[abas] {msg} — pulando sem erro")
    sys.exit(0)


def main():
    if not os.path.exists(ABAS_PATH):
        safe_exit("abas-data.json não existe")

    try:
        from update import metabase_auth, fetch_data, EXCLUDE_PATTERNS, FX_RATES
    except Exception as e:
        safe_exit(f"import de update.py falhou: {e}")

    try:
        abas = json.load(open(ABAS_PATH, encoding="utf-8"))
        settings = json.load(open(SETTINGS_PATH, encoding="utf-8")) if os.path.exists(SETTINGS_PATH) else {}
    except Exception as e:
        safe_exit(f"leitura de JSON falhou: {e}")

    fechados = abas.get("_meses_fechados") or list(abas.get("AN_MESES", []))
    ontem = date.today() - timedelta(days=1)
    mkey = ontem.strftime("%Y-%m")
    if mkey in fechados:
        safe_exit(f"{mkey} é mês FECHADO (fechamento oficial manda) — não sobrescrever")

    # fração do mês decorrida: o valor gravado é o MTD PROJETADO para o mês cheio
    # (consistente com a semântica da coluna * nas abas; correção 04/08 — MTD cru quebrava as projeções)
    import calendar
    dias_mes = calendar.monthrange(ontem.year, ontem.month)[1]
    frac = max(ontem.day / dias_mes, 1.0 / dias_mes)

    try:
        token = metabase_auth()
        # Busca DIA A DIA. A janela do mes inteiro estoura o teto de linhas do
        # Metabase (fetch_data pagina de 2.000 e corta em 52.000) e, como o SQL
        # ordena por "day DESC", o corte descartava os PRIMEIROS dias do mes.
        # Efeito visto em 23/08/26: as abas AdNetwork/AdTech vinham ~32% abaixo
        # do realizado do mes. Mesma correcao ja aplicada no rebuild_real_april.
        rows = []
        _d = ontem.replace(day=1)
        while _d <= ontem:
            rows.extend(fetch_data(token, _d.isoformat(), _d.isoformat()))
            _d += timedelta(days=1)
    except Exception as e:
        safe_exit(f"Metabase falhou: {e}")
    if not rows:
        safe_exit("feed vazio")

    def tr_of(pub, canal):
        cfg = settings.get(pub) or {}
        if canal == "net":
            v = cfg.get("trNetwork")
            return v if isinstance(v, (int, float)) else (0.15 if "SUBPUBLISHER" in pub else 0.0)
        v = cfg.get("trTech")
        return v if isinstance(v, (int, float)) else (0.1 if "SUBPUBLISHER" in pub else 0.0)

    # agrega o mês vigente: AN (network, por adv e por adv×pub) e AT (tech, por pub e pub×adv)
    an_adv, an_pj, at_p, at_pa = {}, {}, {}, {}
    for r in rows:
        try:
            adv = (r.get("advertiser_name") or "").strip()
            pub = (r.get("publisher_name") or "").strip().upper()
            if not adv or not pub:
                continue
            low = adv.lower()
            if any(x in low for x in EXCLUDE_PATTERNS):
                continue
            cost = float(r.get("total_cost") or 0)
            if cost <= 0:
                continue
            fx = FX_RATES.get((r.get("currency_code") or r.get("currency") or "BRL").strip(), 1.0)
            cost_brl = cost * fx
            advU = adv.upper()
            cost_brl = cost_brl / frac  # projeta o MTD para o mês cheio
            if "vtexads" in low:  # AdNetwork (mesma regra do update.py)
                rev = cost_brl * tr_of(pub, "net")
                a = an_adv.setdefault(advU, [0.0, 0.0]); a[0] += cost_brl; a[1] += rev
                pj = an_pj.setdefault(advU, {}).setdefault(pub, [0.0, 0.0]); pj[0] += cost_brl; pj[1] += rev
            else:  # AdTech (inclui sellers)
                rev = cost_brl * tr_of(pub, "tech")
                p = at_p.setdefault(pub, [0.0, 0.0]); p[0] += cost_brl; p[1] += rev
                pa = at_pa.setdefault(pub, {}).setdefault(advU, [0.0, 0.0]); pa[0] += cost_brl; pa[1] += rev
        except Exception:
            continue

    if not an_adv and not at_p:
        safe_exit("agregação vazia")

    n_closed = len(fechados)
    lbl = MES_LBL[ontem.month] + "*"  # * = mês vigente (MTD real via workflow)

    def set_month_col(store, key_new_vals, empty):
        """store: dict de séries alinhadas a meses; garante len == n_closed+1 com o mês vigente na última posição."""
        for k in set(list(store.keys()) + list(key_new_vals.keys())):
            serie = store.get(k)
            if serie is None:
                serie = [list(empty) for _ in range(n_closed)]
            serie = serie[:n_closed]  # descarta vigente antigo, preserva fechados
            v = key_new_vals.get(k, list(empty))
            serie.append([round(v[0]), round(v[1])])
            store[k] = serie
        return store

    try:
        abas["_meses_fechados"] = fechados
        abas["AN_MESES"] = fechados + [mkey]
        abas["AN_LBL"] = [MES_LBL[int(m[5:7])] for m in fechados] + [lbl]
        abas["AN_DATA"] = set_month_col(abas.get("AN_DATA", {}), an_adv, (0, 0))
        abas["AN_PJ"] = {a: {p: [round(v[0]), round(v[1])] for p, v in d.items()} for a, d in an_pj.items()}
        abas["AT_MESES"] = abas["AN_MESES"]
        abas["AT_LBL"] = abas["AN_LBL"]
        abas["AT_P"] = set_month_col(abas.get("AT_P", {}), at_p, (0, 0))
        at_pa_old = abas.get("AT_PA", {})
        new_pa = {}
        for pub in set(list(at_pa_old.keys()) + list(at_pa.keys())):
            advs_old = at_pa_old.get(pub, {})
            advs_new = at_pa.get(pub, {})
            new_pa[pub] = set_month_col(dict(advs_old), advs_new, (0, 0))
        abas["AT_PA"] = new_pa
        abas["_atualizado"] = ontem.isoformat()
    except Exception as e:
        safe_exit(f"montagem falhou: {e}")

    # GUARDA (23/08/26): a coluna do mes vigente e uma PROJECAO do MTD, entao
    # tem que ficar ACIMA do realizado ate ontem. Vir ABAIXO significa que o
    # fetch voltou truncado -- foi exatamente o sintoma que escondeu este bug.
    try:
        _html = open(os.path.join(ROOT, "index.html"), encoding="utf-8").read()
        _mtd = sum(int(x) for x in re.findall(
            r'"[^"]+":\{spendReal:(\d+),revReal:\d+,spendTech:\d+,'
            r'spendNetwork:\d+,revTech:\d+,revNetwork:\d+,publishers:', _html))
        _i = abas["AN_MESES"].index(mkey)
        _j = abas["AT_MESES"].index(mkey)
        _novo = (sum((v[_i][0] if len(v) > _i and v[_i] else 0)
                     for v in abas["AN_DATA"].values())
                 + sum((v[_j][0] if len(v) > _j and v[_j] else 0)
                       for v in abas["AT_P"].values()))
        if _mtd > 0 and _novo < _mtd:
            safe_exit(f"coluna de {mkey} ({_novo:,.0f}) abaixo do realizado do "
                      f"mes ({_mtd:,.0f}) -- fetch provavelmente truncado")
        if _mtd > 0:
            print(f"[abas] check: projecao {_novo:,.0f} vs MTD {_mtd:,.0f} "
                  f"({_novo / _mtd:.2f}x)")
    except SystemExit:
        raise
    except Exception as _e:
        print(f"[abas] aviso: check de consistencia nao rodou ({_e}) -- seguindo")

    tmp = ABAS_PATH + ".tmp"
    json.dump(abas, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, ABAS_PATH)
    print(f"[abas] ✓ {mkey} atualizado: {len(an_adv)} advs network | {len(at_p)} publishers tech")


if __name__ == "__main__":
    main()
