"""
Export the scraped data from SQLite to xlsx / csv / json.

Each table (courses, colleges, offerings) can be exported. The offerings export
is the "joined" view linking courses to the colleges that offer them.
"""

from __future__ import annotations

import io
import json
from typing import List, Tuple

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

import db

TABLES = ("courses", "colleges", "offerings")


def _fetch(table: str, db_path: str = db.DB_PATH) -> Tuple[List[str], List[tuple]]:
    with db.connect(db_path) as conn:
        cur = conn.execute(f"SELECT * FROM {table}")
        headers = [d[0] for d in cur.description]
        rows = [tuple(r) for r in cur.fetchall()]
    return headers, rows


def to_xlsx(tables=TABLES, db_path: str = db.DB_PATH) -> bytes:
    wb = Workbook()
    wb.remove(wb.active)
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill("solid", fgColor="2F5496")
    for table in tables:
        headers, rows = _fetch(table, db_path)
        ws = wb.create_sheet(title=table[:31])
        ws.append(headers)
        for col_idx in range(1, len(headers) + 1):
            cell = ws.cell(row=1, column=col_idx)
            cell.font = header_font
            cell.fill = header_fill
        for r in rows:
            ws.append(list(r))
        ws.freeze_panes = "A2"
        if headers:
            ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"
            for i, h in enumerate(headers, start=1):
                ws.column_dimensions[get_column_letter(i)].width = min(max(len(h) + 4, 12), 60)
    if not wb.sheetnames:
        wb.create_sheet("empty")
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def to_csv(table: str, db_path: str = db.DB_PATH) -> bytes:
    import csv
    headers, rows = _fetch(table, db_path)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def to_json(table: str, db_path: str = db.DB_PATH) -> bytes:
    headers, rows = _fetch(table, db_path)
    data = [dict(zip(headers, r)) for r in rows]
    return json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")


if __name__ == "__main__":
    import sys
    out = sys.argv[1] if len(sys.argv) > 1 else "collegedunia_export.xlsx"
    with open(out, "wb") as fh:
        fh.write(to_xlsx())
    print(f"Wrote {out}")
