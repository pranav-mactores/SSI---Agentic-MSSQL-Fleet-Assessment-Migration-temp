"""
reports/csv_writer.py  –  CSV writing and reading helpers.
"""
import csv, os, re
from typing import Any

def safe_slug(text: str) -> str:
    return re.sub(r'[^A-Za-z0-9_-]', '_', text).strip('_')[:60]

def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        with open(path, "w", newline="", encoding="utf-8") as f:
            f.write("(no data)\n")
        return
    keys = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"    [csv] {os.path.basename(path)}  ({len(rows)} rows)")

def read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []

def isum(rows: list[dict], col: str) -> int:
    total = 0
    for r in rows:
        try: total += int(r.get(col, 0) or 0)
        except (ValueError, TypeError): pass
    return total

def col_val(rows: list[dict], col: str, default: str = "") -> str:
    for r in rows:
        v = str(r.get(col, "") or "").strip()
        if v: return v
    return default
