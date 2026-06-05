#!/usr/bin/env python3
"""ALWAYS truncate index.html at first </html> to prevent any duplication."""
import os

HTML_PATH = os.path.join(os.path.dirname(__file__), "..", "index.html")

with open(HTML_PATH, "r") as f:
    html = f.read()

original_size = len(html)
end = html.find("</html>")
if end > 0:
    html = html[:end + 7]

with open(HTML_PATH, "w") as f:
    f.write(html)

if len(html) < original_size:
    print(f"FIXED: {original_size} → {len(html)} chars (removed {original_size - len(html)} extra chars)")
else:
    print(f"OK: {len(html)} chars, no extra content after </html>")
