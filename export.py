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


def to_analytics_xlsx(db_path: str = db.DB_PATH) -> bytes:
    """A summary workbook: pivots/aggregations for quick analysis."""
    import pandas as pd
    sheets = {}
    with db.connect(db_path) as conn:
        sheets["Courses by stream"] = pd.read_sql_query(
            "SELECT stream_name AS stream, COUNT(*) AS courses, "
            "SUM(colleges_count) AS total_college_slots "
            "FROM courses WHERE stream_name<>'' GROUP BY stream_name "
            "ORDER BY courses DESC", conn)
        sheets["Courses by type"] = pd.read_sql_query(
            "SELECT course_type, level, COUNT(*) AS courses FROM courses "
            "GROUP BY course_type, level ORDER BY courses DESC", conn)
        try:
            sheets["Colleges by city"] = pd.read_sql_query(
                "SELECT city, COUNT(DISTINCT college_id) AS colleges, "
                "COUNT(*) AS offerings FROM offerings WHERE city<>'' "
                "GROUP BY city ORDER BY colleges DESC", conn)
            sheets["Offerings by stream"] = pd.read_sql_query(
                "SELECT c.stream_name AS stream, COUNT(*) AS offerings, "
                "ROUND(AVG(NULLIF(o.fees_amount,0)),0) AS avg_first_year_fee "
                "FROM offerings o JOIN courses c ON o.course_id=c.course_id "
                "GROUP BY c.stream_name ORDER BY offerings DESC", conn)
        except Exception:
            pass
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as xl:
        wrote = False
        for name, df in sheets.items():
            if df is not None and not df.empty:
                df.to_excel(xl, sheet_name=name[:31], index=False)
                wrote = True
        if not wrote:
            pd.DataFrame({"info": ["no data yet"]}).to_excel(xl, sheet_name="empty", index=False)
    return buf.getvalue()


if __name__ == "__main__":
    import sys
    out = sys.argv[1] if len(sys.argv) > 1 else "collegedunia_export.xlsx"
    with open(out, "wb") as fh:
        fh.write(to_xlsx())
    print(f"Wrote {out}")
