#!/usr/bin/env python3
"""Fix duplicated index.html by detecting second <html or <body tag."""
import os

HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "index.html")

with open(HTML_PATH, "r") as f:
    html = f.read()

original_size = len(html)

# Strategy: find the SECOND <html or <body tag — that means duplication
first_html = html.find("<html")
second_html = html.find("<html", first_html + 1) if first_html >= 0 else -1

first_body = html.find("<body")
second_body = html.find("<body", first_body + 1) if first_body >= 0 else -1

cut_point = -1
if second_html > 0:
    cut_point = second_html
elif second_body > 0:
    cut_point = second_body

if cut_point > 0:
    html = html[:cut_point].rstrip()
    # Ensure proper closing tags
    if "</body>" not in html[-50:]:
        html += "\n</body>"
    if "</html>" not in html[-50:]:
        html += "\n</html>"
    with open(HTML_PATH, "w") as f:
        f.write(html)
    print(f"FIXED: {original_size} -> {len(html)} chars (cut at second <html at pos {cut_point})")
else:
    # No duplication, but still ensure clean ending
    end = html.find("</html>")
    if end > 0 and end + 7 < len(html) - 10:
        html = html[:end + 7]
        with open(HTML_PATH, "w") as f:
            f.write(html)
        print(f"TRIMMED: removed {original_size - len(html)} trailing chars")
    else:
        print(f"OK: {len(html)} chars, no duplication")
