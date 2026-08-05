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

# ---------- 5. CONCILIAÇÃO tab Forecast × abas (João 05/08) ----------
# 5a. meta por canal na Composição = OFICIAL (mesma alocação das abas), não mix realizado
COMP_META_V1 = """  const mSTech = mS * rTech;  /* estimate meta split using current ratio */
  const mSNet  = mS * rNet;
  /* Split meta revenue by realized revenue ratio (tech vs network have different TRs) */
  const rRevTech = _realRevTech / (_realRevTotal || 1);
  const rRevNet  = _realRevNet / (_realRevTotal || 1);
  const mRTech = mR * rRevTech;
  const mRNet  = mR * rRevNet;"""
COMP_META_V2 = """  /* Meta por canal = OFICIAL (mesma alocação das abas: onsite+lump 40 | AdTech+60 | off/ins/fees) — João 05/08 */
  let mSNet = mS * (_realSpendNet/(_realSpendTotal||1)), mSTech = mS * (_realSpendTech/(_realSpendTotal||1)), mSOth = null;
  let mRNet = mR * (_realRevNet/(_realRevTotal||1)), mRTech = mR * (_realRevTech/(_realRevTotal||1)), mROth = null;
  let declS = oS, declR = oR;
  try{
    const _k = Object.keys(MONTHS_DATA).find(k=>MONTHS_DATA[k].status==='current');
    if(typeof AN_META!=='undefined' && AN_META[_k] && typeof AT_METAP!=='undefined'){
      const _am = AN_META[_k];
      mSNet = (_am[0]||0)+(_am[7]||0);
      mSTech = Object.values(AT_METAP).reduce((s,d)=>s+(((d[_k]||[0])[0])||0),0);
      mSOth = (_am[2]||0)+(_am[4]||0);
      mRNet = (_am[1]||0)+(_am[8]||0);
      mRTech = Object.values(AT_METAP).reduce((s,d)=>s+(((d[_k]||[0,0])[1])||0),0);
      mROth = (_am[3]||0)+(_am[5]||0)+(_am[6]||0);
    }
    if(typeof anOthBase==='function' && typeof AN_GROUPS!=='undefined' && AN_GROUPS){
      declS = [0,2].reduce((s,oi)=>s+anOthBase(oi).v,0);   /* offsite+instore spend (fallback último mês) */
      declR = [1,3,4].reduce((s,oi)=>s+anOthBase(oi).v,0); /* offsite+instore+fees receita */
    }
  }catch(e){}"""
PATCHES.append(("comp-meta-oficial", COMP_META_V1, COMP_META_V2))

# 5b. linha Others da Composição vira Off/Instore/Fees AN com meta e valores das abas
RO_V1 = """  function rO(label, real, proj) {
    return `<tr style="color:#F59E0B;font-size:11px">
      <td style="padding-left:20px">${label}</td>
      <td class="comp-val">${fB(real)}</td>
      <td class="comp-val">${fB(proj)}</td>
      <td class="comp-val">—</td>
      <td>—</td>
    </tr>`;
  }"""
RO_V2 = """  function rO(label, real, proj, meta) {
    return `<tr style="color:#F59E0B;font-size:11px">
      <td style="padding-left:20px">${label}</td>
      <td class="comp-val">${fB(real)}</td>
      <td class="comp-val">${fB(proj)}</td>
      <td class="comp-val">${meta!=null?fB(meta):'—'}</td>
      <td class="${meta!=null?gC(proj-meta):''}">${meta!=null?gV(proj-meta):'—'}</td>
    </tr>`;
  }"""
PATCHES.append(("comp-rO", RO_V1, RO_V2))
PATCHES.append(("comp-rO-spend", "${rO('Others (Spend)', oS, oS)}",
                "${rO('Off/Instore/Fees AN (Spend)', oS, declS, mSOth)}"))
PATCHES.append(("comp-rO-rev", "${rO('Others (Receita)', oR, oR)}",
                "${rO('Off/Instore/Fees AN (Receita)', oR, declR, mROth)}"))
PATCHES.append(("comp-totalS", "  const totalS = projS + oS;", "  const totalS = projS + declS;"))
PATCHES.append(("comp-totalR", "  const totalR = projR + oR;", "  const totalR = projR + declR;"))

# 5c. calibra o mês vigente das abas ao forecast do produto (mesmo modelo do tab)
AT_APPLY = "function atApplyAll(){const v=parseFloat(document.getElementById('at-mom').value)||0;atState.momDefault=v;atState.grow={};renderAdvTech();}"
CALIB = AT_APPLY + """
/* Calibração: coluna do mês vigente (ago*) escalada para que a soma por canal = forecast do produto (João 05/08) */
document.addEventListener('DOMContentLoaded', function(){
  try{
    if(typeof getTotalForecast!=='function' || !anCurBase()) return;
    const _projS = getTotalForecast();
    const _rN = _realSpendNet/(_realSpendTotal||1), _rT = _realSpendTech/(_realSpendTotal||1);
    const _tSN = _projS*_rN, _tST = _projS*_rT;
    const _tRN = _tSN*BLENDED_TR_NET, _tRT = _tST*BLENDED_TR_TECH;
    let sN=0, rN=0; Object.values(AN_DATA).forEach(v=>{sN+=v[AN_LAST][0]; rN+=v[AN_LAST][1];});
    if(sN>0 && _tSN>0){const f=_tSN/sN, fr=rN>0?_tRN/rN:_tSN/sN;
      Object.values(AN_DATA).forEach(v=>{v[AN_LAST][0]*=f; v[AN_LAST][1]*=fr;});}
    let sT=0, rT=0; Object.values(AT_P).forEach(v=>{sT+=v[AT_LAST][0]; rT+=v[AT_LAST][1];});
    if(sT>0 && _tST>0){const f=_tST/sT, fr=rT>0?_tRT/rT:_tST/sT;
      Object.values(AT_P).forEach(v=>{v[AT_LAST][0]*=f; v[AT_LAST][1]*=fr;});
      Object.values(AT_PA).forEach(d=>Object.values(d).forEach(v=>{v[AT_LAST][0]*=f; v[AT_LAST][1]*=fr;}));}
    console.log('abas calibradas ao forecast do produto ✓');
  }catch(e){console.warn('calibração das abas falhou (mantido linear):',e);}
});"""
PATCHES.append(("abas-calibracao", AT_APPLY, CALIB))

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
AN_ASIS_COMMON = AN_CHIMP + """
  const _hj=new Date(), _dm=new Date(_hj.getFullYear(),_hj.getMonth()+1,0).getDate();
  const _dref=Math.min(Math.max(_hj.getDate()-1,1),_dm), _frac=_dref/_dm;
  const asIsOns=Object.keys(AN_DATA).reduce((s,a)=>s+anBase(a,mi),0);
  const asIsOth=AN_GROUPS?(mi===0?[0,2]:[1,3,4]).reduce((s,oi)=>s+anOthBase(oi).v,0):0;
"""
AN_ASIS_V1 = AN_ASIS_COMMON + "  const asIsTot=asIsOns+asIsOth, mtdOns=asIsOns*_frac;"
AN_ASIS_V2 = AN_ASIS_COMMON + "  const asIsTot=asIsOns+asIsOth, mtdOns=(typeof _realSpendNet!=='undefined'&&anCurBase())?(mi===0?_realSpendNet:_realRevNet):asIsOns*_frac;"
PATCHES.append(("an-asis-v1to2", AN_ASIS_V1, AN_ASIS_V2))
PATCHES.append(("an-asis-calc", AN_CHIMP, AN_ASIS_V2))
AN_KPI_ANCHOR = "  document.getElementById('an-kpis').innerHTML = `"
PATCHES.append((
    "an-asis-cards", AN_KPI_ANCHOR,
    AN_KPI_ANCHOR + """
    <div class="card" style="padding:14px"><div style="font-size:11px;color:#6B7785">REALIZADO ${(AN_LBL[AN_LAST]||'').toUpperCase()} · ATÉ DIA ${_dref}</div><div style="font-size:22px;font-weight:700">R$ ${anFmt(mtdOns)}</div><div style="font-size:11px;color:#6B7785">onsite MTD · ritmo R$ ${anFmt(mtdOns/_dref)}/dia</div></div>
    <div class="card" style="padding:14px"><div style="font-size:11px;color:#6B7785">FORECAST AS-IS · MÊS CHEIO</div><div style="font-size:22px;font-weight:700">R$ ${anFmt(asIsTot)}</div><div style="font-size:11px;color:#6B7785">ritmo atual, sem MoM (onsite ${anFmt(asIsOns)} + outros ${anFmt(asIsOth)})</div></div>""",
))
AT_CHIMP = "  const chImp=mi===0?ch.ts:ch.trv;"
AT_ASIS_COMMON = AT_CHIMP + """
  const _hj=new Date(), _dm=new Date(_hj.getFullYear(),_hj.getMonth()+1,0).getDate();
  const _dref=Math.min(Math.max(_hj.getDate()-1,1),_dm), _frac=_dref/_dm;
"""
AT_ASIS_V1 = AT_ASIS_COMMON + "  const asIsTot=atPubs().reduce((s,pu)=>s+(AT_P[pu]?AT_P[pu][AT_LAST][mi]:0),0), mtdTot=asIsTot*_frac;"
AT_ASIS_V2 = AT_ASIS_COMMON + "  const asIsTot=atPubs().reduce((s,pu)=>s+(AT_P[pu]?AT_P[pu][AT_LAST][mi]:0),0), mtdTot=(typeof _realSpendTech!=='undefined'&&anCurBase())?(mi===0?_realSpendTech:_realRevTech):asIsTot*_frac;"
PATCHES.append(("at-asis-v1to2", AT_ASIS_V1, AT_ASIS_V2))
PATCHES.append(("at-asis-calc", AT_CHIMP, AT_ASIS_V2))
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
            "an-note-v1to2", "at-note-v1to2", "reqG-v2to3", "an-asis-v1to2", "at-asis-v1to2"}
PATCHES.append(("fee-detail-v1to2", FEE_V1, FEE_V2))
PATCHES.append(("fee-detail", FEE_ROW_ANCHOR, FEE_V2))

# ---------- 6. julho fechado: detalhe por publisher + totais oficiais (João 05/08) ----------
JUL_ANCHOR = "anLoadScen();"
JUL_NEW = "anLoadScen();\n/* julho fechado — detalhe OFICIAL por publisher (fechamento validado) + corrige totais contaminados com 1-2/ago — João 05/08 */\ntry{\n  CLOSED_DETAIL['2026-07']={real:{\"Pharma\":{\"spendReal\":5305807,\"revReal\":949681,\"spendTech\":761707,\"spendNetwork\":4544100,\"revTech\":74714,\"revNetwork\":874967,\"publishers\":{\"PANVEL\":{\"spendReal\":808604,\"revReal\":200358,\"spendTech\":211117,\"spendNetwork\":597487,\"revTech\":21112,\"revNetwork\":179246,\"trTech\":0.1,\"trNetwork\":0.3},\"PAGUE MENOS\":{\"spendReal\":996613,\"revReal\":89695,\"spendTech\":145692,\"spendNetwork\":850922,\"revTech\":13112,\"revNetwork\":76583,\"trTech\":0.09,\"trNetwork\":0.09},\"DROGARIA SÃO PAULO ADS\":{\"spendReal\":3500590,\"revReal\":659628,\"spendTech\":404898,\"spendNetwork\":3095691,\"revTech\":40490,\"revNetwork\":619138,\"trTech\":0.1,\"trNetwork\":0.2}}},\"Electronics\":{\"spendReal\":5097044,\"revReal\":360486,\"spendTech\":5038851,\"spendNetwork\":58193,\"revTech\":350628,\"revNetwork\":9857,\"publishers\":{\"CASAS BAHIA ADS\":{\"spendReal\":3038577,\"revReal\":256789,\"spendTech\":3025981,\"spendNetwork\":12596,\"revTech\":255719,\"revNetwork\":1071,\"trTech\":0.085,\"trNetwork\":0.085},\"BEMOL\":{\"spendReal\":417714,\"revReal\":83543,\"spendTech\":414156,\"spendNetwork\":3558,\"revTech\":82831,\"revNetwork\":712,\"trTech\":0.2,\"trNetwork\":0.2},\"FAST SHOP\":{\"spendReal\":145221,\"revReal\":9682,\"spendTech\":121009,\"spendNetwork\":24212,\"revTech\":6050,\"revNetwork\":3632,\"trTech\":0.05,\"trNetwork\":0.15},\"KABUM\":{\"spendReal\":1445361,\"revReal\":1619,\"spendTech\":1445170,\"spendNetwork\":191,\"revTech\":1584,\"revNetwork\":34,\"trTech\":0.001,\"trNetwork\":0.18},\"AMERICANAS\":{\"spendReal\":50170,\"revReal\":8853,\"spendTech\":32534,\"spendNetwork\":17636,\"revTech\":4444,\"revNetwork\":4409,\"trTech\":0.137,\"trNetwork\":0.25}}},\"Long Tail\":{\"spendReal\":1333757,\"revReal\":197205,\"spendTech\":1299121,\"spendNetwork\":34635,\"revTech\":191415,\"revNetwork\":5790,\"publishers\":{\"GROWD ADS\":{\"spendReal\":89354,\"revReal\":10391,\"spendTech\":59223,\"spendNetwork\":30131,\"revTech\":5871,\"revNetwork\":4520,\"trTech\":0.099,\"trNetwork\":0.15},\"CLUBBI\":{\"spendReal\":7094,\"revReal\":709,\"spendTech\":7094,\"spendNetwork\":0,\"revTech\":709,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.0},\"HAPVIDA - PUBLISHER\":{\"spendReal\":1229539,\"revReal\":184431,\"spendTech\":1229539,\"spendNetwork\":0,\"revTech\":184431,\"revNetwork\":0,\"trTech\":0.15,\"trNetwork\":0.3},\"SUPER XTRA - PUBLISHER\":{\"spendReal\":779,\"revReal\":117,\"spendTech\":779,\"spendNetwork\":0,\"revTech\":117,\"revNetwork\":0,\"trTech\":0.15,\"trNetwork\":0.15},\"LOJAS IMPÉRIO ADS\":{\"spendReal\":1920,\"revReal\":268,\"spendTech\":390,\"spendNetwork\":1530,\"revTech\":39,\"revNetwork\":230,\"trTech\":0.1,\"trNetwork\":0.15},\"DECATLHON ADS\":{\"spendReal\":1715,\"revReal\":172,\"spendTech\":1715,\"spendNetwork\":0,\"revTech\":172,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.0},\"PROMOFARMA - PUBLISHER\":{\"spendReal\":3356,\"revReal\":1117,\"spendTech\":382,\"spendNetwork\":2974,\"revTech\":76,\"revNetwork\":1041,\"trTech\":0.2,\"trNetwork\":0.35}}},\"Beauty\":{\"spendReal\":569324,\"revReal\":32630,\"spendTech\":517393,\"spendNetwork\":51931,\"revTech\":23283,\"revNetwork\":9348,\"publishers\":{\"SEPHORA ADS PUBLISHER\":{\"spendReal\":569324,\"revReal\":32630,\"spendTech\":517393,\"spendNetwork\":51931,\"revTech\":23283,\"revNetwork\":9348,\"trTech\":0.045,\"trNetwork\":0.18}}},\"LATAM\":{\"spendReal\":507755,\"revReal\":64928,\"spendTech\":507755,\"spendNetwork\":0,\"revTech\":64928,\"revNetwork\":0,\"publishers\":{\"OLIMPICA CO - PUBLISHER\":{\"spendReal\":45800,\"revReal\":2290,\"spendTech\":45800,\"spendNetwork\":0,\"revTech\":2290,\"revNetwork\":0,\"trTech\":0.05,\"trNetwork\":0.15},\"FARMACITY CONECT\":{\"spendReal\":447709,\"revReal\":60441,\"spendTech\":447709,\"spendNetwork\":0,\"revTech\":60441,\"revNetwork\":0,\"trTech\":0.135,\"trNetwork\":0.1},\"LOCATEL\":{\"spendReal\":6045,\"revReal\":816,\"spendTech\":6045,\"spendNetwork\":0,\"revTech\":816,\"revNetwork\":0,\"trTech\":0.135,\"trNetwork\":0.0},\"SHOPSTAR PUBLISHER\":{\"spendReal\":3025,\"revReal\":605,\"spendTech\":3025,\"spendNetwork\":0,\"revTech\":605,\"revNetwork\":0,\"trTech\":0.2,\"trNetwork\":0.4},\"DIA ADS\":{\"spendReal\":5161,\"revReal\":774,\"spendTech\":5161,\"spendNetwork\":0,\"revTech\":774,\"revNetwork\":0,\"trTech\":0.15,\"trNetwork\":0.0},\"FARMACITYQA\":{\"spendReal\":15,\"revReal\":2,\"spendTech\":15,\"spendNetwork\":0,\"revTech\":2,\"revNetwork\":0,\"trTech\":0.135,\"trNetwork\":0.0}}},\"Home Center\":{\"spendReal\":326326,\"revReal\":30553,\"spendTech\":326326,\"spendNetwork\":0,\"revTech\":30553,\"revNetwork\":0,\"publishers\":{\"MADEIRA ADS\":{\"spendReal\":207996,\"revReal\":18720,\"spendTech\":207996,\"spendNetwork\":0,\"revTech\":18720,\"revNetwork\":0,\"trTech\":0.09,\"trNetwork\":0.09},\"LEROY MERLIN\":{\"spendReal\":118330,\"revReal\":11833,\"spendTech\":118330,\"spendNetwork\":0,\"revTech\":11833,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.16}}},\"Others\":{\"spendReal\":263162,\"revReal\":83570,\"spendTech\":16075,\"spendNetwork\":247087,\"revTech\":653,\"revNetwork\":82917,\"publishers\":{\"ELETRO ANGELONI\":{\"spendReal\":18,\"revReal\":0,\"spendTech\":18,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.3},\"KABUM STAGING\":{\"spendReal\":658,\"revReal\":0,\"spendTech\":658,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"TOTALPASS - PUBLISHER - POC\":{\"spendReal\":10269,\"revReal\":1500,\"spendTech\":269,\"spendNetwork\":10000,\"revTech\":0,\"revNetwork\":1500,\"trTech\":0.0,\"trNetwork\":0.15},\"BUSCAPE\":{\"spendReal\":2985,\"revReal\":298,\"spendTech\":2985,\"spendNetwork\":0,\"revTech\":298,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.0},\"TENDA ATACADO ADS HML\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":2,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"VR ADS HML\":{\"spendReal\":272,\"revReal\":0,\"spendTech\":272,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"POC SANTANDER ESFERA - PUBLISHER\":{\"spendReal\":3,\"revReal\":0,\"spendTech\":3,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"ZOOM\":{\"spendReal\":2537,\"revReal\":254,\"spendTech\":2537,\"spendNetwork\":0,\"revTech\":254,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.0},\"EITRI PUBLISHER\":{\"spendReal\":0,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"AMERICANAS STAGING\":{\"spendReal\":29,\"revReal\":0,\"spendTech\":29,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"RETAIL MEDIA CASAS BAHIA STAGING\":{\"spendReal\":3229,\"revReal\":0,\"spendTech\":3229,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"VIP COMMERCE - SUBPUBLISHER SANDBOX\":{\"spendReal\":21,\"revReal\":2,\"spendTech\":21,\"spendNetwork\":0,\"revTech\":2,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.0},\"SUPER ANGELONI\":{\"spendReal\":6517,\"revReal\":1185,\"spendTech\":2567,\"spendNetwork\":3951,\"revTech\":0,\"revNetwork\":1185,\"trTech\":0.0,\"trNetwork\":0.3},\"BUSCAPE STAGING\":{\"spendReal\":5,\"revReal\":0,\"spendTech\":5,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"ON CITY - PUBLISHER\":{\"spendReal\":0,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"AUCHAN POC\":{\"spendReal\":53,\"revReal\":0,\"spendTech\":53,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"VTEX GROCERY\":{\"spendReal\":7,\"revReal\":1,\"spendTech\":7,\"spendNetwork\":0,\"revTech\":1,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.0},\"HORTIFRUTI BR - NATURAL DA TERRA - PUBLISHER\":{\"spendReal\":1575,\"revReal\":0,\"spendTech\":1575,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"VIPCOMMERCE STAGING - SUBPUBLISHER\":{\"spendReal\":52,\"revReal\":0,\"spendTech\":52,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"CVC - PUBLISHER\":{\"spendReal\":64,\"revReal\":0,\"spendTech\":64,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"COBASI ADS\":{\"spendReal\":618,\"revReal\":0,\"spendTech\":618,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"FASTSTORE HML - PUBLISHER\":{\"spendReal\":0,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"LEROY MERLIN STAGING\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":2,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.1,\"trNetwork\":0.0},\"BEMOL QA (STAGING)\":{\"spendReal\":20,\"revReal\":0,\"spendTech\":20,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"PICPAY PUBLISHER\":{\"spendReal\":1087,\"revReal\":98,\"spendTech\":1087,\"spendNetwork\":0,\"revTech\":98,\"revNetwork\":0,\"trTech\":0.09,\"trNetwork\":0.0},\"CLUBBI STAGING\":{\"spendReal\":0,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"VTEX.COM\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":2,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"VTEX ELETRO STORE\":{\"spendReal\":0,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":0,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.111,\"trNetwork\":0.0},\"GBARBOSA - PUBLISHER\":{\"spendReal\":29101,\"revReal\":7275,\"spendTech\":0,\"spendNetwork\":29101,\"revTech\":0,\"revNetwork\":7275,\"trTech\":0.0,\"trNetwork\":0.25},\"DROGAL ADS\":{\"spendReal\":88607,\"revReal\":44304,\"spendTech\":0,\"spendNetwork\":88607,\"revTech\":0,\"revNetwork\":44304,\"trTech\":0.0,\"trNetwork\":0.5},\"GBARBOSA\":{\"spendReal\":9097,\"revReal\":2274,\"spendTech\":0,\"spendNetwork\":9097,\"revTech\":0,\"revNetwork\":2274,\"trTech\":0.0,\"trNetwork\":0.25},\"VENÂNCIO\":{\"spendReal\":27079,\"revReal\":8124,\"spendTech\":0,\"spendNetwork\":27079,\"revTech\":0,\"revNetwork\":8124,\"trTech\":0.0,\"trNetwork\":0.3},\"SUPER MUFFATO DELIVERY\":{\"spendReal\":4156,\"revReal\":831,\"spendTech\":0,\"spendNetwork\":4156,\"revTech\":0,\"revNetwork\":831,\"trTech\":0.0,\"trNetwork\":0.2},\"MERCANTIL NOVA ERA LTDA\":{\"spendReal\":1378,\"revReal\":689,\"spendTech\":0,\"spendNetwork\":1378,\"revTech\":0,\"revNetwork\":689,\"trTech\":0.0,\"trNetwork\":0.5},\"MERCANTIL ATACADO\":{\"spendReal\":1309,\"revReal\":655,\"spendTech\":0,\"spendNetwork\":1309,\"revTech\":0,\"revNetwork\":655,\"trTech\":0.0,\"trNetwork\":0.5},\"FARMÁCIA INDIANA ADS\":{\"spendReal\":6775,\"revReal\":3388,\"spendTech\":0,\"spendNetwork\":6775,\"revTech\":0,\"revNetwork\":3388,\"trTech\":0.0,\"trNetwork\":0.5},\"ÉPOCA COSMÉTICOS ADS\":{\"spendReal\":1945,\"revReal\":194,\"spendTech\":0,\"spendNetwork\":1945,\"revTech\":0,\"revNetwork\":194,\"trTech\":0.0,\"trNetwork\":0.1},\"ZONA SUL ADS\":{\"spendReal\":475,\"revReal\":95,\"spendTech\":0,\"spendNetwork\":475,\"revTech\":0,\"revNetwork\":95,\"trTech\":0.0,\"trNetwork\":0.2},\"DROGARIA CATARINENSE\":{\"spendReal\":147,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":147,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.0},\"MEVO FARMA\":{\"spendReal\":4,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":4,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.3},\"BISTURI DISTRIBUIDORA DE MATERIAL HOSPITALAR LTDA\":{\"spendReal\":1,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":1,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.1},\"PETLOVE ADS\":{\"spendReal\":45883,\"revReal\":6882,\"spendTech\":0,\"spendNetwork\":45883,\"revTech\":0,\"revNetwork\":6882,\"trTech\":0.0,\"trNetwork\":0.15},\"REDETOP - SUBPUBLISHER\":{\"spendReal\":46,\"revReal\":7,\"spendTech\":0,\"spendNetwork\":46,\"revTech\":0,\"revNetwork\":7,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPERVILLE SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":28,\"revReal\":4,\"spendTech\":0,\"spendNetwork\":28,\"revTech\":0,\"revNetwork\":4,\"trTech\":0.0,\"trNetwork\":0.15},\"IPANEMA SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":48,\"revReal\":7,\"spendTech\":0,\"spendNetwork\":48,\"revTech\":0,\"revNetwork\":7,\"trTech\":0.0,\"trNetwork\":0.15},\"ENXUTO - SUBPUBLISHER\":{\"spendReal\":36,\"revReal\":5,\"spendTech\":0,\"spendNetwork\":36,\"revTech\":0,\"revNetwork\":5,\"trTech\":0.0,\"trNetwork\":0.15},\"ALABARCE DELIVERY - SUBPUBLISHER\":{\"spendReal\":12,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":12,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.15},\"SAN MICHEL - SUBPUBLISHER\":{\"spendReal\":16,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":16,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.1},\"HIROTA - SUBPUBLISHER\":{\"spendReal\":508,\"revReal\":76,\"spendTech\":0,\"spendNetwork\":508,\"revTech\":0,\"revNetwork\":76,\"trTech\":0.0,\"trNetwork\":0.15},\"OXAN SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":16,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":16,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.15},\"ATACADISTA MEGA - SUBPUBLISHER\":{\"spendReal\":102,\"revReal\":15,\"spendTech\":0,\"spendNetwork\":102,\"revTech\":0,\"revNetwork\":15,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPER G SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":10,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":10,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.15},\"DELTASUPER - SUBPUBLISHER\":{\"spendReal\":186,\"revReal\":28,\"spendTech\":0,\"spendNetwork\":186,\"revTech\":0,\"revNetwork\":28,\"trTech\":0.0,\"trNetwork\":0.15},\"FERREIRA EM CASA - SUBPUBLISHER\":{\"spendReal\":98,\"revReal\":15,\"spendTech\":0,\"spendNetwork\":98,\"revTech\":0,\"revNetwork\":15,\"trTech\":0.0,\"trNetwork\":0.15},\"MEGA SUPERMERCADO - SUBPUBLISHER\":{\"spendReal\":20,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":20,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.1},\"VILLARREAL ONLINE - SUBPUBLISHER\":{\"spendReal\":48,\"revReal\":7,\"spendTech\":0,\"spendNetwork\":48,\"revTech\":0,\"revNetwork\":7,\"trTech\":0.0,\"trNetwork\":0.15},\"VILA RICA - SUBPUBLISHER\":{\"spendReal\":4,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":4,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"PINHEIRO - SUBPUBLISHER\":{\"spendReal\":282,\"revReal\":42,\"spendTech\":0,\"spendNetwork\":282,\"revTech\":0,\"revNetwork\":42,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPERMERCADO MANAÍRA\":{\"spendReal\":50,\"revReal\":8,\"spendTech\":0,\"spendNetwork\":50,\"revTech\":0,\"revNetwork\":8,\"trTech\":0.0,\"trNetwork\":0.15},\"CARVALHO SUPERSHOP - SUBPUBLISHER\":{\"spendReal\":72,\"revReal\":7,\"spendTech\":0,\"spendNetwork\":72,\"revTech\":0,\"revNetwork\":7,\"trTech\":0.0,\"trNetwork\":0.1},\"SÃO JOÃO - SUBPUBLISHER\":{\"spendReal\":14,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":14,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.1},\"REDE COMPRAS - SUBPUBLISHER\":{\"spendReal\":166,\"revReal\":25,\"spendTech\":0,\"spendNetwork\":166,\"revTech\":0,\"revNetwork\":25,\"trTech\":0.0,\"trNetwork\":0.15},\"CARROSSEL NO LAR - SUBPUBLISHER\":{\"spendReal\":124,\"revReal\":19,\"spendTech\":0,\"spendNetwork\":124,\"revTech\":0,\"revNetwork\":19,\"trTech\":0.0,\"trNetwork\":0.15},\"GAROTO ATACADO - SUBPUBLISHER\":{\"spendReal\":140,\"revReal\":21,\"spendTech\":0,\"spendNetwork\":140,\"revTech\":0,\"revNetwork\":21,\"trTech\":0.0,\"trNetwork\":0.15},\"FAVORITO SUPERMERCADOS\":{\"spendReal\":40,\"revReal\":6,\"spendTech\":0,\"spendNetwork\":40,\"revTech\":0,\"revNetwork\":6,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPERMERCADOS DAOLIO\":{\"spendReal\":70,\"revReal\":10,\"spendTech\":0,\"spendNetwork\":70,\"revTech\":0,\"revNetwork\":10,\"trTech\":0.0,\"trNetwork\":0.15},\"FRADEILHA - SUBPUBLISHER\":{\"spendReal\":32,\"revReal\":5,\"spendTech\":0,\"spendNetwork\":32,\"revTech\":0,\"revNetwork\":5,\"trTech\":0.0,\"trNetwork\":0.15},\"CENTRAL - SUBPUBLISHER\":{\"spendReal\":108,\"revReal\":16,\"spendTech\":0,\"spendNetwork\":108,\"revTech\":0,\"revNetwork\":16,\"trTech\":0.0,\"trNetwork\":0.15},\"CAMBUI - SUBPUBLISHER\":{\"spendReal\":16,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":16,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.15},\"FONSECA SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":114,\"revReal\":17,\"spendTech\":0,\"spendNetwork\":114,\"revTech\":0,\"revNetwork\":17,\"trTech\":0.0,\"trNetwork\":0.15},\"AGROMIX PET CENTER - SUBPUBLISHER\":{\"spendReal\":8,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":8,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPERMERCADO DO ROBERTO - SUBPUBLISHER\":{\"spendReal\":20,\"revReal\":3,\"spendTech\":0,\"spendNetwork\":20,\"revTech\":0,\"revNetwork\":3,\"trTech\":0.0,\"trNetwork\":0.15},\"SILVA INDAIA - SUBPUBLISHER\":{\"spendReal\":22,\"revReal\":3,\"spendTech\":0,\"spendNetwork\":22,\"revTech\":0,\"revNetwork\":3,\"trTech\":0.0,\"trNetwork\":0.15},\"2 BEM CASA - SUBPUBLISHER\":{\"spendReal\":8,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":8,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"FÊNIX SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":48,\"revReal\":7,\"spendTech\":0,\"spendNetwork\":48,\"revTech\":0,\"revNetwork\":7,\"trTech\":0.0,\"trNetwork\":0.15},\"DINIZ SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":174,\"revReal\":26,\"spendTech\":0,\"spendNetwork\":174,\"revTech\":0,\"revNetwork\":26,\"trTech\":0.0,\"trNetwork\":0.15},\"PRÓ BRAZILIAN - SUBPUBLISHER\":{\"spendReal\":42,\"revReal\":6,\"spendTech\":0,\"spendNetwork\":42,\"revTech\":0,\"revNetwork\":6,\"trTech\":0.0,\"trNetwork\":0.15},\"LIVELO - PUBLISHER\":{\"spendReal\":11866,\"revReal\":4746,\"spendTech\":0,\"spendNetwork\":11866,\"revTech\":0,\"revNetwork\":4746,\"trTech\":0.0,\"trNetwork\":0.4},\"ROYAL SUPERMERCADO - SUBPUBLISHER\":{\"spendReal\":16,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":16,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.15},\"CENTERPAO ATE VOCE - SUBPUBLISHER\":{\"spendReal\":6,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":6,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.1},\"FASOUTO - VIPCOMMERCE SUBPUBLISHER\":{\"spendReal\":36,\"revReal\":5,\"spendTech\":0,\"spendNetwork\":36,\"revTech\":0,\"revNetwork\":5,\"trTech\":0.0,\"trNetwork\":0.15},\"FEDERZONI - SUBPUBLISHER\":{\"spendReal\":18,\"revReal\":3,\"spendTech\":0,\"spendNetwork\":18,\"revTech\":0,\"revNetwork\":3,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPERSUL - SUBPUBLISHER\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":2,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.1},\"FEIRA NOVA EM CASA - SUBPUBLISHER\":{\"spendReal\":66,\"revReal\":10,\"spendTech\":0,\"spendNetwork\":66,\"revTech\":0,\"revNetwork\":10,\"trTech\":0.0,\"trNetwork\":0.15},\"ATALAIA RAÇÕES - SUBPUBLISHER\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":2,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"SANTAREM SUPERMERCADO - SUBPUBLISHER\":{\"spendReal\":10,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":10,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPER VALE - SUBPUBLISHER\":{\"spendReal\":12,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":12,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.1},\"BRAMIL EM CASA - SUBPUBLISHER\":{\"spendReal\":462,\"revReal\":69,\"spendTech\":0,\"spendNetwork\":462,\"revTech\":0,\"revNetwork\":69,\"trTech\":0.0,\"trNetwork\":0.15},\"MERCADO AJUBÁ\":{\"spendReal\":4,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":4,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"JAGUARE\":{\"spendReal\":20,\"revReal\":3,\"spendTech\":0,\"spendNetwork\":20,\"revTech\":0,\"revNetwork\":3,\"trTech\":0.0,\"trNetwork\":0.15},\"ARMAZEM DO GRÃO - SUBPUBLISHER\":{\"spendReal\":166,\"revReal\":25,\"spendTech\":0,\"spendNetwork\":166,\"revTech\":0,\"revNetwork\":25,\"trTech\":0.0,\"trNetwork\":0.15},\"HIPER BERGAMINI - SUBPUBLISHER\":{\"spendReal\":44,\"revReal\":7,\"spendTech\":0,\"spendNetwork\":44,\"revTech\":0,\"revNetwork\":7,\"trTech\":0.0,\"trNetwork\":0.15},\"VIOLETA EXPRESS - SUBPUBLISHER\":{\"spendReal\":472,\"revReal\":71,\"spendTech\":0,\"spendNetwork\":472,\"revTech\":0,\"revNetwork\":71,\"trTech\":0.0,\"trNetwork\":0.15},\"HIPER NATASHA - SUBPUBLISHER\":{\"spendReal\":6,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":6,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"PECITO - SUBPUBLISHER\":{\"spendReal\":28,\"revReal\":4,\"spendTech\":0,\"spendNetwork\":28,\"revTech\":0,\"revNetwork\":4,\"trTech\":0.0,\"trNetwork\":0.15},\"PÉROLA - SUBPUBLISHER\":{\"spendReal\":50,\"revReal\":8,\"spendTech\":0,\"spendNetwork\":50,\"revTech\":0,\"revNetwork\":8,\"trTech\":0.0,\"trNetwork\":0.15},\"VILA SUL - SUBPUBLISHER\":{\"spendReal\":82,\"revReal\":12,\"spendTech\":0,\"spendNetwork\":82,\"revTech\":0,\"revNetwork\":12,\"trTech\":0.0,\"trNetwork\":0.15},\"DELIVERY HORTISABOR - SUBPUBLISHER\":{\"spendReal\":14,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":14,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.15},\"GUARANI SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":38,\"revReal\":6,\"spendTech\":0,\"spendNetwork\":38,\"revTech\":0,\"revNetwork\":6,\"trTech\":0.0,\"trNetwork\":0.15},\"VIEIRAO - SUBPUBLISHER\":{\"spendReal\":4,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":4,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"SIMPATIA ONLINE - SUBPUBLISHER\":{\"spendReal\":98,\"revReal\":10,\"spendTech\":0,\"spendNetwork\":98,\"revTech\":0,\"revNetwork\":10,\"trTech\":0.0,\"trNetwork\":0.1},\"IRMÃOS SILVA - SUBPUBLISHER\":{\"spendReal\":28,\"revReal\":4,\"spendTech\":0,\"spendNetwork\":28,\"revTech\":0,\"revNetwork\":4,\"trTech\":0.0,\"trNetwork\":0.15},\"UNICOMPRA - SUBPUBLISHER\":{\"spendReal\":32,\"revReal\":5,\"spendTech\":0,\"spendNetwork\":32,\"revTech\":0,\"revNetwork\":5,\"trTech\":0.0,\"trNetwork\":0.15},\"TONIN SUPERATACADO - SUBPUBLISHER\":{\"spendReal\":124,\"revReal\":19,\"spendTech\":0,\"spendNetwork\":124,\"revTech\":0,\"revNetwork\":19,\"trTech\":0.0,\"trNetwork\":0.15},\"SÃO RAFAEL SUPERMERCADO - SUBPUBLISHER\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":2,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"LUZITANA EM CASA - SUBPUBLISHER\":{\"spendReal\":12,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":12,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.1},\"NORDESTÃO SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":194,\"revReal\":29,\"spendTech\":0,\"spendNetwork\":194,\"revTech\":0,\"revNetwork\":29,\"trTech\":0.0,\"trNetwork\":0.15},\"CALEGARIS EM CASA - SUBPUBLISHER\":{\"spendReal\":4,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":4,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.1},\"REDEMARKET - SUBPUBLISHER\":{\"spendReal\":34,\"revReal\":3,\"spendTech\":0,\"spendNetwork\":34,\"revTech\":0,\"revNetwork\":3,\"trTech\":0.0,\"trNetwork\":0.1},\"COOCERQUI SUPERMERCADO - SUBPUBLISHER\":{\"spendReal\":18,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":18,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.1},\"PERIM - SUBPUBLISHER\":{\"spendReal\":100,\"revReal\":10,\"spendTech\":0,\"spendNetwork\":100,\"revTech\":0,\"revNetwork\":10,\"trTech\":0.0,\"trNetwork\":0.1},\"SORRISO - SUBPUBLISHER\":{\"spendReal\":24,\"revReal\":4,\"spendTech\":0,\"spendNetwork\":24,\"revTech\":0,\"revNetwork\":4,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPER ECONÔMICO - SUBPUBLISHER\":{\"spendReal\":24,\"revReal\":2,\"spendTech\":0,\"spendNetwork\":24,\"revTech\":0,\"revNetwork\":2,\"trTech\":0.0,\"trNetwork\":0.1},\"BUONA GENTE - SUBPUBLISHER\":{\"spendReal\":28,\"revReal\":4,\"spendTech\":0,\"spendNetwork\":28,\"revTech\":0,\"revNetwork\":4,\"trTech\":0.0,\"trNetwork\":0.15},\"STYLLUS SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":14,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":14,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.1},\"GARCIA SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":38,\"revReal\":6,\"spendTech\":0,\"spendNetwork\":38,\"revTech\":0,\"revNetwork\":6,\"trTech\":0.0,\"trNetwork\":0.15},\"DROGARIAS FERREIRA - SUBPUBLISHER\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":2,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"DE ANGELINA SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":20,\"revReal\":3,\"spendTech\":0,\"spendNetwork\":20,\"revTech\":0,\"revNetwork\":3,\"trTech\":0.0,\"trNetwork\":0.15},\"FLEX - SUBPUBLISHER\":{\"spendReal\":100,\"revReal\":15,\"spendTech\":0,\"spendNetwork\":100,\"revTech\":0,\"revNetwork\":15,\"trTech\":0.0,\"trNetwork\":0.15},\"NIPPO - SUBPUBLISHER\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":2,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"VISCARDI - SUBPUBLISHER\":{\"spendReal\":8,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":8,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.1},\"TRENTO SUPERMERCADOS - SUBPUBLISHER\":{\"spendReal\":6,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":6,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"PORTO EM CASA - SUBPUBLISHER\":{\"spendReal\":44,\"revReal\":7,\"spendTech\":0,\"spendNetwork\":44,\"revTech\":0,\"revNetwork\":7,\"trTech\":0.0,\"trNetwork\":0.15},\"SUPER LOCAL - SUBPUBLISHER\":{\"spendReal\":2,\"revReal\":0,\"spendTech\":0,\"spendNetwork\":2,\"revTech\":0,\"revNetwork\":0,\"trTech\":0.0,\"trNetwork\":0.15},\"JB DE FRUTAL - SUBPUBLISHER\":{\"spendReal\":26,\"revReal\":3,\"spendTech\":0,\"spendNetwork\":26,\"revTech\":0,\"revNetwork\":3,\"trTech\":0.0,\"trNetwork\":0.1},\"SUPERMERCADO DONA MARIA - SUBPUBLISHER\":{\"spendReal\":4,\"revReal\":1,\"spendTech\":0,\"spendNetwork\":4,\"revTech\":0,\"revNetwork\":1,\"trTech\":0.0,\"trNetwork\":0.15},\"CENTRAL DELIVERY - SUBPUBLISHER\":{\"spendReal\":40,\"revReal\":6,\"spendTech\":0,\"spendNetwork\":40,\"revTech\":0,\"revNetwork\":6,\"trTech\":0.0,\"trNetwork\":0.15}}},\"Groceries\":{\"spendReal\":124686,\"revReal\":17267,\"spendTech\":56008,\"spendNetwork\":68678,\"revTech\":5600,\"revNetwork\":11667,\"publishers\":{\"TENDA ATACADO - PUBLISHER\":{\"spendReal\":95391,\"revReal\":11513,\"spendTech\":55923,\"spendNetwork\":39468,\"revTech\":5592,\"revNetwork\":5920,\"trTech\":0.1,\"trNetwork\":0.15},\"SUPERMERCADO SAVEGNAGO\":{\"spendReal\":5310,\"revReal\":794,\"spendTech\":61,\"spendNetwork\":5250,\"revTech\":6,\"revNetwork\":787,\"trTech\":0.1,\"trNetwork\":0.15},\"SUPER NOSSO\":{\"spendReal\":10330,\"revReal\":1547,\"spendTech\":25,\"spendNetwork\":10306,\"revTech\":1,\"revNetwork\":1546,\"trTech\":0.05,\"trNetwork\":0.15},\"PREZUNIC\":{\"spendReal\":13654,\"revReal\":3414,\"spendTech\":0,\"spendNetwork\":13654,\"revTech\":0,\"revNetwork\":3414,\"trTech\":0.0,\"trNetwork\":0.25}}}}};\n  if(MONTHS_DATA['2026-07']){MONTHS_DATA['2026-07'].realSpend=13527861;MONTHS_DATA['2026-07'].realRev=1736320;}\n  Object.entries({\"Pharma\":[5305807,949681],\"Electronics\":[5097044,360486],\"Long Tail\":[1333757,197205],\"Beauty\":[569324,32630],\"LATAM\":[507755,64928],\"Home Center\":[326326,30553],\"Others\":[263162,83570],\"Groceries\":[124686,17267]}).forEach(function(e){if(typeof REAL_MONTHLY!=='undefined'&&REAL_MONTHLY[e[0]])REAL_MONTHLY[e[0]]['2026-07']={spend:e[1][0],rev:e[1][1]};});\n}catch(e){console.warn('detalhe julho falhou',e);}"
PATCHES.append(("closed-jul-detail", JUL_ANCHOR, JUL_NEW))


# metas por segmento de julho (arquivo oficial; close nao arquiva segMetas) — João 05/08
PATCHES.append(("closed-jul-segmetas", "}catch(e){console.warn('detalhe julho falhou',e);}", "}catch(e){console.warn('detalhe julho falhou',e);}\ntry{if(MONTHS_DATA['2026-07'])MONTHS_DATA['2026-07'].segMetas={\"Pharma\":6170294,\"Electronics\":7022839,\"Long Tail\":1965906,\"Beauty\":624119,\"LATAM\":1567472,\"Home Center\":260623,\"Others\":19811,\"Grocery\":79188};}catch(e){}"))


# ---------- 7. Composição do mês fechado: metas nas sub-linhas (João 05/08) ----------
PATCHES.append(("closed-meta-helpers", "    function gV(v){return (v>=0?'+':'')+fB(v)}", "    function gV(v){return (v>=0?'+':'')+fB(v)}\n    const _adsMS = m.segMetas ? Object.values(m.segMetas).reduce((s,v)=>s+v,0) : null;\n    const _othMS = _adsMS!=null ? m.metaSpend - _adsMS : null;\n    const _adsMR = (m.revAdsMeta!=null)? m.revAdsMeta : null;\n    const _othMR = (m.revOthersMeta!=null)? m.revOthersMeta : null;\n    const mc = (real, meta)=> meta!=null ? '<td class=\"comp-val\">'+fB(meta)+'</td><td class=\"'+gC(real-meta)+'\">'+gV(real-meta)+'</td><td class=\"comp-val\">'+fP(pct(real, meta))+'</td>' : '<td class=\"comp-val\">\\u2014</td><td>\\u2014</td><td class=\"comp-val\">\\u2014</td>';"))
PATCHES.append(("closed-row-ads", "        <tr><td>Ad Spend (Ads)</td><td class=\"comp-val\">${fB(m.realSpend)}</td><td class=\"comp-val\">—</td><td>—</td><td class=\"comp-val\">—</td></tr>", "        <tr><td>Ad Spend (Ads)</td><td class=\"comp-val\">${fB(m.realSpend)}</td>${mc(m.realSpend,_adsMS)}</tr>"))
PATCHES.append(("closed-row-oths", "'<tr style=\"color:#F59E0B;font-size:11px\"><td style=\"padding-left:12px\">Others</td><td class=\"comp-val\">'+fB(oSp)+'</td><td class=\"comp-val\">—</td><td>—</td><td class=\"comp-val\">—</td></tr>'", "'<tr style=\"color:#F59E0B;font-size:11px\"><td style=\"padding-left:12px\">Others</td><td class=\"comp-val\">'+fB(oSp)+'</td>'+mc(oSp,_othMS)+'</tr>'"))
PATCHES.append(("closed-row-revads", "        <tr><td>Receita (Ads)</td><td class=\"comp-val\">${fB(m.realRev)}</td><td class=\"comp-val\">—</td><td>—</td><td class=\"comp-val\">—</td></tr>", "        <tr><td>Receita (Ads)</td><td class=\"comp-val\">${fB(m.realRev)}</td>${mc(m.realRev,_adsMR)}</tr>"))
PATCHES.append(("closed-row-othrv", "'<tr style=\"color:#F59E0B;font-size:11px\"><td style=\"padding-left:12px\">Others Rev</td><td class=\"comp-val\">'+fB(oRv)+'</td><td class=\"comp-val\">—</td><td>—</td><td class=\"comp-val\">—</td></tr>'", "'<tr style=\"color:#F59E0B;font-size:11px\"><td style=\"padding-left:12px\">Others Rev</td><td class=\"comp-val\">'+fB(oRv)+'</td>'+mc(oRv,_othMR)+'</tr>'"))
PATCHES.append(("closed-jul-revmeta", "try{if(MONTHS_DATA['2026-07'])MONTHS_DATA['2026-07'].segMetas={\"Pharma\":6170294,\"Electronics\":7022839,\"Long Tail\":1965906,\"Beauty\":624119,\"LATAM\":1567472,\"Home Center\":260623,\"Others\":19811,\"Grocery\":79188};}catch(e){}", "try{if(MONTHS_DATA['2026-07'])MONTHS_DATA['2026-07'].segMetas={\"Pharma\":6170294,\"Electronics\":7022839,\"Long Tail\":1965906,\"Beauty\":624119,\"LATAM\":1567472,\"Home Center\":260623,\"Others\":19811,\"Grocery\":79188};}catch(e){}\ntry{if(MONTHS_DATA['2026-07']){MONTHS_DATA['2026-07'].revAdsMeta=2523118;MONTHS_DATA['2026-07'].revOthersMeta=283500;}}catch(e){}"))


def main():
    if not os.path.exists(HTML):
        safe_exit("index.html não encontrado")
    html = open(HTML, encoding="utf-8").read()
    orig = html
    aplicados, pulados = [], []
    # se o marcador v1 ainda existe e o v1to2 não casou, NÃO reaplica (evita duplicação)
    SKIP_IF = {"an-asis-calc": "mtdOns=asIsOns*_frac", "at-asis-calc": "mtdTot=asIsTot*_frac"}
    for pid, old, new in PATCHES:
        if new in html:
            pulados.append(pid)
            continue
        marker = SKIP_IF.get(pid)
        if marker and marker in html:
            pulados.append(pid + "(v1-presente)")
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
