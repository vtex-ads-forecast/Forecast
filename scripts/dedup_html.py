#!/usr/bin/env python3
"""Fix duplicated index.html by extracting the first complete document."""
import os

HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "index.html")

with open(HTML_PATH, "r") as f:
    html = f.read()

scripts = html.count("<script>")
print(f"Input: {len(html)} chars, {scripts} script tags")

if scripts > 1:
    end = html.find("</html>")
    if end > 0:
        html = html[:end + 7]
        with open(HTML_PATH, "w") as f:
            f.write(html)
        print(f"Fixed: {len(html)} chars, {html.count('<script>')} script tag")
else:
    print("OK — no duplication detected")
