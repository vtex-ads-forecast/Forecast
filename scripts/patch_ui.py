#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Patch de UI das abas AdNetwork/AdTech (João 04/08):
  1. FIX matemática do MoM: expoente n agora conta do MÊS-BASE (vigente nas abas)
     até o mês-alvo — antes contava de junho e sobre-compunha (ex.: ^3 em vez de ^1).
  2. Cenário salvo: botão 💾 salva MoM padrão + % por linha + mês-alvo no navegador
     (localStorage); recarrega automático ao abrir. ↺ limpa.
  3. Header: cards REALIZADO (MTD) e FORECAST AS-IS (mês cheio, sem MoM) nas 2 abas.
  4. Linha FEES aberta por tipo (AdNetwork Fee / Fixed Fee) com detalhe por conta.

Idempotente: cada patch é pulado se já aplicado. FAIL-SAFE: qualquer anomalia => exit 0.
"""
import os
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
HTML = os.path.join(ROOT, "index.html")


def safe_exit(msg):
    print(f"[patch_ui] {msg} — pulando sem erro")
    sys.exit(0)


# (id, old, new) — old precisa ocorrer EXATAMENTE 1x; se `new` já está no arquivo, pula
PATCHES = []

# ---------- 1. FIX do expoente n ----------
PATCHES.append((
    "anN",
    "function anN(){ return Object.keys(AN_MLBL).indexOf(anState.alvo)+(AN_HASJUL?0:1); }",
    "function anN(){ const b=AN_MESES[AN_LAST].split('-').map(Number), t=anState.alvo.split('-').map(Number); return Math.max((t[0]*12+t[1])-(b[0]*12+b[1]),0); }",
))
PATCHES.append((
    "atN",
    "function atN(){return Object.keys(AT_MLBL).indexOf(atState.alvo)+(AT_HASJUL?0:1);}",
    "function atN(){const b=AT_MESES[AT_LAST].split('-').map(Number), t=atState.alvo.split('-').map(Number); return Math.max((t[0]*12+t[1])-(b[0]*12+b[1]),0);}",
))
PATCHES.append((
    "reqG-n0",
    "const reqG = (base,metaV)=>{ if(metaV<=0) return '—'; if(base<=0) return 'novo';",
    "const reqG = (base,metaV)=>{ if(n<=0) return '—'; if(metaV<=0) return '—'; if(base<=0) return 'novo';",
))

# ---------- 2. cenário salvo (AN) + dados de fees por tipo ----------
AN_STATE_ANCHOR = "               open:{onsA:false, onsC:false, off:false, ins:false, fee:false}};"
AN_SCEN_BLOCK = AN_STATE_ANCHOR + """
/* fees por tipo — fonte: aba Others do fechamento oficial (04/08) */
const AN_FEE_DET={'2026-07':[['AdNetwork Fee',67000,'Novo Nordisk 40k · Zoetis 10k · Brace 11k · Petlove 6k'],['Fixed Fee',58700,'Globo 28,7k · Zona Sul 25k · Clubbi 5k']]};
/* cenário salvo no navegador (João 04/08) */
function anSaveScen(){try{localStorage.setItem('anScen',JSON.stringify({momDefault:anState.momDefault,grow:anState.grow,growOth:anState.growOth,alvo:anState.alvo,ts:Date.now()}));var el=document.getElementById('an-scen-info');if(el)el.textContent='✓ salvo '+new Date().toLocaleString('pt-BR').slice(0,16);}catch(e){}}
function anLoadScen(){try{var s=JSON.parse(localStorage.getItem('anScen')||'null');if(!s)return;if(typeof s.momDefault==='number')anState.momDefault=s.momDefault;anState.grow=s.grow||{};anState.growOth=s.growOth||{};if(s.alvo&&AN_MLBL[s.alvo])anState.alvo=s.alvo;var i=document.getElementById('an-mom');if(i)i.value=anState.momDefault;var el=document.getElementById('an-scen-info');if(el)el.textContent='cenário salvo '+new Date(s.ts).toLocaleString('pt-BR').slice(0,16);}catch(e){}}
function anClearScen(){try{localStorage.removeItem('anScen');}catch(e){}anState.grow={};anState.growOth={};anState.momDefault=15;var i=document.getElementById('an-mom');if(i)i.value=15;var el=document.getElementById('an-scen-info');if(el)el.textContent='';renderAdvNet();}
anLoadScen();"""
PATCHES.append(("an-scen", AN_STATE_ANCHOR, AN_SCEN_BLOCK))

# ---------- 2b. cenário salvo (AT) ----------
AT_STATE_ANCHOR = "let atState={mode:'spend',alvo:atDefaultAlvo(),momDefault:15,grow:{},open:{}};"
AT_SCEN_BLOCK = AT_STATE_ANCHOR + """
function atSaveScen(){try{localStorage.setItem('atScen',JSON.stringify({momDefault:atState.momDefault,grow:atState.grow,alvo:atState.alvo,ts:Date.now()}));var el=document.getElementById('at-scen-info');if(el)el.textContent='✓ salvo '+new Date().toLocaleString('pt-BR').slice(0,16);}catch(e){}}
function atLoadScen(){try{var s=JSON.parse(localStorage.getItem('atScen')||'null');if(!s)return;if(typeof s.momDefault==='number')atState.momDefault=s.momDefault;atState.grow=s.grow||{};if(s.alvo&&AT_MLBL[s.alvo])atState.alvo=s.alvo;var i=document.getElementById('at-mom');if(i)i.value=atState.momDefault;var el=document.getElementById('at-scen-info');if(el)el.textContent='cenário salvo '+new Date(s.ts).toLocaleString('pt-BR').slice(0,16);}catch(e){}}
function atClearScen(){try{localStorage.removeItem('atScen');}catch(e){}atState.grow={};atState.momDefault=15;var i=document.getElementById('at-mom');if(i)i.value=15;var el=document.getElementById('at-scen-info');if(el)el.textContent='';renderAdvTech();}
atLoadScen();"""
PATCHES.append(("at-scen", AT_STATE_ANCHOR, AT_SCEN_BLOCK))

# ---------- 2c. botões ----------
BTN_STYLE_SAVE = "padding:6px 12px;border:1px solid #F71963;border-radius:6px;background:#fff;color:#F71963;font-size:12.5px;cursor:pointer;font-family:inherit"
BTN_STYLE_CLR = "padding:6px 10px;border:1px solid #E0E0E6;border-radius:6px;background:#fff;color:#6B7785;font-size:12.5px;cursor:pointer;font-family:inherit"
AN_BTN_ANCHOR = '<button onclick="anApplyAll()" style="padding:6px 12px;border:none;border-radius:6px;background:#F71963;color:#fff;font-size:12.5px;cursor:pointer;font-family:inherit">Aplicar a todos</button>'
PATCHES.append((
    "an-btn", AN_BTN_ANCHOR,
    AN_BTN_ANCHOR
    + f'<button onclick="anSaveScen()" style="{BTN_STYLE_SAVE}">\U0001f4be Salvar cenário</button>'
    + f'<button onclick="anClearScen()" title="limpa o cenário salvo e volta ao padrão" style="{BTN_STYLE_CLR}">↺</button>'
    + '<span id="an-scen-info" style="font-size:11px;color:#0A7D4F"></span>',
))
AT_BTN_ANCHOR = '<button onclick="atApplyAll()" style="padding:6px 12px;border:none;border-radius:6px;background:#F71963;color:#fff;font-size:12.5px;cursor:pointer;font-family:inherit">Aplicar a todos</button>'
PATCHES.append((
    "at-btn", AT_BTN_ANCHOR,
    AT_BTN_ANCHOR
    + f'<button onclick="atSaveScen()" style="{BTN_STYLE_SAVE}">\U0001f4be Salvar cenário</button>'
    + f'<button onclick="atClearScen()" title="limpa o cenário salvo e volta ao padrão" style="{BTN_STYLE_CLR}">↺</button>'
    + '<span id="at-scen-info" style="font-size:11px;color:#0A7D4F"></span>',
))

# ---------- 3. header: realizado + as-is ----------
AN_CHIMP = "  const chImp = mi===0?ch.ts:ch.trv;"
PATCHES.append((
    "an-asis-calc", AN_CHIMP,
    AN_CHIMP + """
  const _hj=new Date(), _dm=new Date(_hj.getFullYear(),_hj.getMonth()+1,0).getDate();
  const _dref=Math.min(Math.max(_hj.getDate()-1,1),_dm), _frac=_dref/_dm;
  const asIsOns=Object.keys(AN_DATA).reduce((s,a)=>s+anBase(a,mi),0);
  const asIsOth=AN_GROUPS?(mi===0?[0,2]:[1,3,4]).reduce((s,oi)=>s+anOthBase(oi).v,0):0;
  const asIsTot=asIsOns+asIsOth, mtdOns=asIsOns*_frac;""",
))
AN_KPI_ANCHOR = "  document.getElementById('an-kpis').innerHTML = `"
PATCHES.append((
    "an-asis-cards", AN_KPI_ANCHOR,
    AN_KPI_ANCHOR + """
    <div class="card" style="padding:14px"><div style="font-size:11px;color:#6B7785">REALIZADO ${(AN_LBL[AN_LAST]||'').toUpperCase()} · ATÉ DIA ${_dref}</div><div style="font-size:22px;font-weight:700">R$ ${anFmt(mtdOns)}</div><div style="font-size:11px;color:#6B7785">onsite MTD · ritmo R$ ${anFmt(mtdOns/_dref)}/dia</div></div>
    <div class="card" style="padding:14px"><div style="font-size:11px;color:#6B7785">FORECAST AS-IS · MÊS CHEIO</div><div style="font-size:22px;font-weight:700">R$ ${anFmt(asIsTot)}</div><div style="font-size:11px;color:#6B7785">ritmo atual, sem MoM (onsite ${anFmt(asIsOns)} + outros ${anFmt(asIsOth)})</div></div>""",
))
AT_CHIMP = "  const chImp=mi===0?ch.ts:ch.trv;"
PATCHES.append((
    "at-asis-calc", AT_CHIMP,
    AT_CHIMP + """
  const _hj=new Date(), _dm=new Date(_hj.getFullYear(),_hj.getMonth()+1,0).getDate();
  const _dref=Math.min(Math.max(_hj.getDate()-1,1),_dm), _frac=_dref/_dm;
  const asIsTot=atPubs().reduce((s,pu)=>s+(AT_P[pu]?AT_P[pu][AT_LAST][mi]:0),0), mtdTot=asIsTot*_frac;""",
))
AT_KPI_ANCHOR = "  document.getElementById('at-kpis').innerHTML=`"
PATCHES.append((
    "at-asis-cards", AT_KPI_ANCHOR,
    AT_KPI_ANCHOR + """
    <div class="card" style="padding:14px"><div style="font-size:11px;color:#6B7785">REALIZADO ${(AT_LBL[AT_LAST]||'').toUpperCase()} · ATÉ DIA ${_dref}</div><div style="font-size:22px;font-weight:700">R$ ${atFmt(mtdTot)}</div><div style="font-size:11px;color:#6B7785">MTD · ritmo R$ ${atFmt(mtdTot/_dref)}/dia</div></div>
    <div class="card" style="padding:14px"><div style="font-size:11px;color:#6B7785">FORECAST AS-IS · MÊS CHEIO</div><div style="font-size:22px;font-weight:700">R$ ${atFmt(asIsTot)}</div><div style="font-size:11px;color:#6B7785">ritmo atual, sem MoM aplicado</div></div>""",
))

# ---------- 3b. grid de 4 → 3 colunas (agora são 6 cards) ----------
PATCHES.append((
    "an-kpi-css",
    "#an-kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}",
    "#an-kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px}",
))
PATCHES.append((
    "at-kpi-css",
    "#at-kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}",
    "#at-kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px}",
))

# ---------- 4. fees por tipo ----------
PATCHES.append((
    "othbase-key",
    "    if(Math.abs(v)>0.5) return {v:v, mes:AN_LBL[i]};",
    "    if(Math.abs(v)>0.5) return {v:v, mes:AN_LBL[i], key:AN_MESES[i]};",
))
FEE_ROW_ANCHOR = "      <td style=\"font-weight:600\">${anFmt(pj)} <span class=\"an-mut\">meta ${anFmt(metaV)}</span></td></tr>`;"
PATCHES.append((
    "fee-detail", FEE_ROW_ANCHOR,
    FEE_ROW_ANCHOR + """
    if(anState.open[key]&&feeMode&&typeof AN_FEE_DET!=='undefined'&&ob.key&&AN_FEE_DET[ob.key]){
      AN_FEE_DET[ob.key].forEach(d=>{html+=`<tr><td class="an-name an-mut" style="padding-left:34px">↳ ${d[0]}</td><td colspan="${AN_LBL.length}" class="an-mut" style="font-size:11px">R$ ${anFmt(d[1])} em ${ob.mes} · ${d[2]}</td><td></td><td></td></tr>`});
    }""",
))


def main():
    if not os.path.exists(HTML):
        safe_exit("index.html não encontrado")
    html = open(HTML, encoding="utf-8").read()
    orig = html
    aplicados, pulados = [], []
    for pid, old, new in PATCHES:
        if new in html:
            pulados.append(pid)
            continue
        n = html.count(old)
        if n != 1:
            safe_exit(f"patch '{pid}': âncora ocorre {n}x (esperado 1) — NADA foi alterado")
        html = html.replace(old, new, 1)
        aplicados.append(pid)
    if html == orig:
        print(f"[patch_ui] ✓ nada a fazer (já aplicados: {', '.join(pulados)})")
        return
    tmp = HTML + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(html)
    os.replace(tmp, HTML)
    print(f"[patch_ui] ✓ aplicados: {', '.join(aplicados)}" + (f" | já ok: {', '.join(pulados)}" if pulados else ""))


if __name__ == "__main__":
    main()
