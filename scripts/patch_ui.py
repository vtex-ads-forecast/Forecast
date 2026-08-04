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

# ---------- 1b. MoM ABSOLUTO: % aplica direto sobre o forecast do mês (João 04/08) ----------
AN_N_NEW = "function anN(){ const b=AN_MESES[AN_LAST].split('-').map(Number), t=anState.alvo.split('-').map(Number); return Math.max((t[0]*12+t[1])-(b[0]*12+b[1]),0); }"
AN_HELPERS_V1 = AN_N_NEW + """
function anFrac(){const h=new Date(),dm=new Date(h.getFullYear(),h.getMonth()+1,0).getDate();return Math.min(Math.max(h.getDate()-1,1),dm)/dm;}
function anCurBase(){const h=new Date();return AN_MESES[AN_LAST]===h.getFullYear()+'-'+String(h.getMonth()+1).padStart(2,'0');}
function anFactor(g,n){if(n>0)return Math.pow(1+g,n);if(!anCurBase())return 1;const f=anFrac();return f+(1-f)*(1+g);}"""
AN_HELPERS_V2 = AN_N_NEW + """
function anFrac(){const h=new Date(),dm=new Date(h.getFullYear(),h.getMonth()+1,0).getDate();return Math.min(Math.max(h.getDate()-1,1),dm)/dm;}
function anCurBase(){const h=new Date();return AN_MESES[AN_LAST]===h.getFullYear()+'-'+String(h.getMonth()+1).padStart(2,'0');}
function anFactor(g,n){return 1+g;} /* % absoluto sobre o forecast do mês (João 04/08) */"""
PATCHES.append(("an-helpers-v1to2", AN_HELPERS_V1, AN_HELPERS_V2))
PATCHES.append(("an-helpers", AN_N_NEW, AN_HELPERS_V2))
AT_N_NEW = "function atN(){const b=AT_MESES[AT_LAST].split('-').map(Number), t=atState.alvo.split('-').map(Number); return Math.max((t[0]*12+t[1])-(b[0]*12+b[1]),0);}"
AT_HELPERS_V1 = AT_N_NEW + """
function atFactor(g,n){if(n>0)return Math.pow(1+g,n);const h=new Date();if(AT_MESES[AT_LAST]!==h.getFullYear()+'-'+String(h.getMonth()+1).padStart(2,'0'))return 1;const f=anFrac();return f+(1-f)*(1+g);}"""
AT_HELPERS_V2 = AT_N_NEW + """
function atFactor(g,n){return 1+g;} /* % absoluto sobre o forecast do mês (João 04/08) */"""
PATCHES.append(("at-helpers-v1to2", AT_HELPERS_V1, AT_HELPERS_V2))
PATCHES.append(("at-helpers", AT_N_NEW, AT_HELPERS_V2))
for pid, old, new in [
    ("an-f1", "t += anOthBase(oi).v*Math.pow(1+g,n);", "t += anOthBase(oi).v*anFactor(g,n);"),
    ("an-f2", "ps+=anBase(a,0)*Math.pow(1+g,n); pr+=anBase(a,1)*Math.pow(1+g,n);",
              "ps+=anBase(a,0)*anFactor(g,n); pr+=anBase(a,1)*anFactor(g,n);"),
    ("an-f3", "return s+anBase(a,mi)*Math.pow(1+g,n)},0);", "return s+anBase(a,mi)*anFactor(g,n)},0);"),
    ("an-f4", "const pj=b*Math.pow(1+g/100,n);", "const pj=b*anFactor(g/100,n);"),
    ("an-f5", "const pj=ob.v*Math.pow(1+g/100,n);", "const pj=ob.v*anFactor(g/100,n);"),
    ("an-f6", "const pj=lista.reduce((s,a)=>{const gg=(anState.grow[a]!==undefined?anState.grow[a]:anState.momDefault)/100;return s+anBase(a,mi)*Math.pow(1+gg,n)},0);",
              "const pj=lista.reduce((s,a)=>{const gg=(anState.grow[a]!==undefined?anState.grow[a]:anState.momDefault)/100;return s+anBase(a,mi)*anFactor(gg,n)},0);"),
    ("an-f7", "const projA=comAg.reduce((s,a)=>{const gg=(anState.grow[a]!==undefined?anState.grow[a]:anState.momDefault)/100;return s+anBase(a,mi)*Math.pow(1+gg,n)},0);",
              "const projA=comAg.reduce((s,a)=>{const gg=(anState.grow[a]!==undefined?anState.grow[a]:anState.momDefault)/100;return s+anBase(a,mi)*anFactor(gg,n)},0);"),
    ("at-f1", "t+=base*Math.pow(1+g,n);});return t;}", "t+=base*atFactor(g,n);});return t;}"),
    ("at-f2", "return s+b*Math.pow(1+gg,n)},0);", "return s+b*atFactor(gg,n)},0);"),
    ("at-f3", "const pj=base*Math.pow(1+g/100,n);", "const pj=base*atFactor(g/100,n);"),
    ("at-f4", "const pjA=v[AT_LAST][mi]*Math.pow(1+g/100,n);", "const pjA=v[AT_LAST][mi]*atFactor(g/100,n);"),
]:
    PATCHES.append((pid, old, new))

# notas: fórmula final = forecast do mês × (1+MoM)
NOTE_AN_V0 = "projeção = base × (1+MoM)^${n} por anunciante"
NOTE_AN_V1 = "projeção = ${n>0?'base × (1+MoM)^'+n:'MTD + restante do mês × (1+MoM)'} por anunciante"
NOTE_AN_V2 = "projeção = forecast do mês × (1+MoM) por anunciante"
NOTE_AT_V0 = "projeção = base × (1+MoM)^${n} por publisher"
NOTE_AT_V1 = "projeção = ${n>0?'base × (1+MoM)^'+n:'MTD + restante do mês × (1+MoM)'} por publisher"
NOTE_AT_V2 = "projeção = forecast do mês × (1+MoM) por publisher"
PATCHES.append(("an-note-v1to2", NOTE_AN_V1, NOTE_AN_V2))
PATCHES.append(("an-note", NOTE_AN_V0, NOTE_AN_V2))
PATCHES.append(("at-note-v1to2", NOTE_AT_V1, NOTE_AT_V2))
PATCHES.append(("at-note", NOTE_AT_V0, NOTE_AT_V2))

# reqG v3: crescimento necessário p/ meta = % simples sobre o forecast do mês
REQG_V1 = "const reqG = (base,metaV)=>{ if(n<=0) return '—'; if(metaV<=0) return '—'; if(base<=0) return 'novo'; return ((Math.pow(metaV/base,1/n)-1)*100).toFixed(1)+'%/mês'; };"
REQG_V2 = "const reqG = (base,metaV)=>{ if(base<=0) return metaV>0?'novo':'—'; if(metaV<=0) return '—'; if(n<=0){ if(!anCurBase()) return '—'; const f=anFrac(); return (((metaV/base-f)/(1-f)-1)*100).toFixed(1)+'% no resto do mês'; } return ((Math.pow(metaV/base,1/n)-1)*100).toFixed(1)+'%/mês'; };"
REQG_V3 = "const reqG = (base,metaV)=>{ if(base<=0) return metaV>0?'novo':'—'; if(metaV<=0) return '—'; return '+'+((metaV/base-1)*100).toFixed(1)+'%'; };"
PATCHES.append(("reqG-v2to3", REQG_V2, REQG_V3))
PATCHES.append(("reqG-v3", REQG_V1, REQG_V3))

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
FEE_V1 = FEE_ROW_ANCHOR + """
    if(anState.open[key]&&feeMode&&typeof AN_FEE_DET!=='undefined'&&ob.key&&AN_FEE_DET[ob.key]){
      AN_FEE_DET[ob.key].forEach(d=>{html+=`<tr><td class="an-name an-mut" style="padding-left:34px">↳ ${d[0]}</td><td colspan="${AN_LBL.length}" class="an-mut" style="font-size:11px">R$ ${anFmt(d[1])} em ${ob.mes} · ${d[2]}</td><td></td><td></td></tr>`});
    }"""
FEE_V2 = FEE_ROW_ANCHOR + """
    if(anState.open[key]&&feeMode&&typeof AN_FEE_DET!=='undefined'){
      var _fk=(ob.key&&AN_FEE_DET[ob.key])?ob.key:Object.keys(AN_FEE_DET).sort().pop();
      if(_fk&&AN_FEE_DET[_fk]){var _flbl=AN_LBL[AN_MESES.indexOf(_fk)]||_fk;
        AN_FEE_DET[_fk].forEach(d=>{html+=`<tr><td class="an-name an-mut" style="padding-left:34px">↳ ${d[0]}</td><td colspan="${AN_LBL.length}" class="an-mut" style="font-size:11px">R$ ${anFmt(d[1])} em ${_flbl}${_fk!==ob.key?' — última composição conhecida (base atual sem quebra por tipo)':''} · ${d[2]}</td><td></td><td></td></tr>`});
      }
    }"""
# converte v1→v2 se v1 presente (opcional); senão aplica v2 direto na âncora
OPTIONAL = {"fee-detail-v1to2", "reqG-n0", "an-helpers-v1to2", "at-helpers-v1to2",
            "an-note-v1to2", "at-note-v1to2", "reqG-v2to3"}
PATCHES.append(("fee-detail-v1to2", FEE_V1, FEE_V2))
PATCHES.append(("fee-detail", FEE_ROW_ANCHOR, FEE_V2))


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
            if pid in OPTIONAL and n == 0:
                pulados.append(pid + "(n/a)")
                continue
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
