import json
from decimal import Decimal


def title(text):
    print(f"\n{'=' * 90}\n{text}\n{'=' * 90}")


def step(text):
    print(f"\n--- {text}")


def show(value):
    if not isinstance(value, (dict, list)):
        value = list(value)
    print(json.dumps(value, indent=2, ensure_ascii=False, default=str))


def table(rows, columns=None):
    rows = list(rows)
    if not rows:
        print("(sin resultados)")
        return
    columns = columns or list(rows[0].keys())
    widths = {c: max(len(str(c)), *(len(_cell(r.get(c))) for r in rows)) for c in columns}
    print(" | ".join(str(c).ljust(widths[c]) for c in columns))
    print("-+-".join("-" * widths[c] for c in columns))
    for row in rows:
        print(" | ".join(_cell(row.get(c)).ljust(widths[c]) for c in columns))


def _cell(value):
    if isinstance(value, (float, Decimal)):
        return f"{value:,.2f}"
    return str(value)
