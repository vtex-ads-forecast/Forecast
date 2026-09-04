#!/usr/bin/env python3
"""Corrige duplicacoes no index.html.

1) Documento duplicado: corta no SEGUNDO <html ou <body (comportamento original).
2) Declaracao const NOME = ... repetida na raiz do script: mantem a PRIMEIRA
   e remove as seguintes.

O item (2) existe por causa do incidente de 01-03/09/2026: a virada de mes
gravou um segundo const REAL_CLOSED_2026_08 (com dados de 1-2/set) ao lado do
fechamento real de agosto. Duas declaracoes const com o mesmo nome sao
SyntaxError: o bloco <script> inteiro para de executar e o dashboard sobe sem
nenhum dado. A versao anterior deste script so detectava documento duplicado,
entao o erro passou tres dias sem ser visto.
"""
import os
import re
import sys

NL = chr(10)
HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "index.html")

with open(HTML_PATH, "r", encoding="utf-8") as f:
    html = f.read()

original_size = len(html)
mudou = False

# ---------------- 1. documento duplicado ----------------
first_html = html.find("<html")
second_html = html.find("<html", first_html + 1) if first_html >= 0 else -1
first_body = html.find("<body")
second_body = html.find("<body", first_body + 1) if first_body >= 0 else -1

cut = second_html if second_html > 0 else (second_body if second_body > 0 else -1)
if cut > 0:
    html = html[:cut].rstrip()
    if "</body>" not in html[-50:]:
        html += NL + "</body>"
    if "</html>" not in html[-50:]:
        html += NL + "</html>"
    mudou = True
    print(f"[dedup] documento duplicado: cortado em {cut}")

# ---------------- 2. const duplicado ----------------
DECL = re.compile("^const[ ]+([A-Za-z_$][A-Za-z0-9_$]*)[ ]*=", re.M)


def fim_do_statement(txt, ini):
    """Fim (exclusivo) do statement iniciado em ini, por balanco de chaves."""
    d = 0
    i = ini
    n = len(txt)
    while i < n:
        c = txt[i]
        if c in "{[":
            d += 1
        elif c in "}]":
            d -= 1
            if d == 0:
                j = txt.find(NL, i)
                return n if j < 0 else j + 1
        elif d == 0 and c == NL:
            return i + 1          # declaracao de uma linha, sem bloco
        i += 1
    return -1


for _ in range(50):
    vistos = {}
    alvo = None
    for m in DECL.finditer(html):
        nome = m.group(1)
        if nome in vistos:
            alvo = (nome, m.start(), vistos[nome])
            break
        vistos[nome] = m.start()
    if alvo is None:
        break
    nome, ini, ini_1 = alvo
    fim = fim_do_statement(html, ini)
    if fim < 0:
        print(f"[dedup] ERRO: nao delimitei o const {nome} duplicado -- nada removido")
        sys.exit(1)
    dup = html[ini:fim]
    if dup.count("{") != dup.count("}") or dup.count("[") != dup.count("]"):
        print(f"[dedup] ERRO: bloco do const {nome} desbalanceado -- nada removido")
        sys.exit(1)
    fim_1 = fim_do_statement(html, ini_1)
    tam_1 = (fim_1 - ini_1) if fim_1 > 0 else 0
    print(f"[dedup] const {nome} duplicado: mantida a 1a ({tam_1} chars), "
          f"removida a 2a ({len(dup)} chars)")
    html = html[:ini] + html[fim:]
    mudou = True
else:
    print("[dedup] ERRO: mais de 50 duplicacoes -- abortando")
    sys.exit(1)

# ---------------- guarda final ----------------
nomes = [m.group(1) for m in DECL.finditer(html)]
repetidos = sorted({n for n in nomes if nomes.count(n) > 1})
if repetidos:
    print(f"[dedup] ERRO: ainda ha const repetido: {repetidos}")
    sys.exit(1)

if mudou:
    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[dedup] OK: {original_size} -> {len(html)} chars")
else:
    print("[dedup] OK: nada duplicado")
