"""Seed content fingerprints on every tracked table — offline, zero requests.

Rows scraped before `freshness.py` existed carry no `content_hash`, so the first
refresh sweep would treat all 900k+ of them as changed and re-fetch everything.
Seeding the hashes now is what makes that first sweep cheap.

    python freshness_backfill.py            # report only, no writes
    python freshness_backfill.py --apply    # seed the hashes
    python freshness_backfill.py --status   # what the log has recorded so far

Nothing is deleted or overwritten: this adds five columns and computes a hash
from values already stored.
"""
from __future__ import annotations

import sys
import time

import freshness as fr

# (module holding the db path, table, primary-key columns)
# Only tables a refresh sweep can actually re-observe are listed. Progress and
# job bookkeeping tables are deliberately absent — they are internal state, not
# scraped facts, and tracking them would fill the log with noise.
TRACKED = [
    ("db", "colleges", ("college_id",)),
    ("db", "courses", ("course_id",)),
    ("db", "offerings", ("course_id", "college_id")),
    ("db", "college_courses", ("college_id", "course_name")),
    ("db", "colleges_directory", ("college_id",)),
    ("sa_db", "sa_programs", ("program_id",)),
    ("sa_db", "sa_universities", ("university_id",)),
    ("sa_db", "sa_countries", ("country_code",)),
    ("sa_db", "sa_program_exams", ("program_id", "short_form")),
    ("sa_db", "sa_scholarships", ("scholarship_id",)),
    # NB: agency_id, not agency — the display name is not the key.
    ("sa_db", "sa_university_rankings", ("university_id", "agency_id", "year", "stream")),
    ("sa_db", "sa_university_courses", ("university_id", "course_id")),
    ("sa_db", "sa_program_fees", ("program_id", "year_added", "fee_type")),
    ("sa_db", "sa_program_scholarships", ("program_id", "scholarship_id")),
    ("cf_db", "cf_courses", ("course_id",)),
    ("cf_db", "cf_offerings", ("course_id", "college_id")),
]


def _paths():
    import db
    import sa_db
    import cf_db
    return {"db": db.DB_PATH, "sa_db": sa_db.SA_DB_PATH, "cf_db": cf_db.CF_DB_PATH}


def _exists(path, table) -> bool:
    conn = fr.connect(path)
    try:
        return bool(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)).fetchone())
    finally:
        conn.close()


def _pk_ok(path, table, pk):
    """A primary key that does not exist would silently hash the wrong thing."""
    conn = fr.connect(path)
    try:
        cols = set(fr.table_columns(conn, table))
        return [c for c in pk if c not in cols]
    finally:
        conn.close()


def status() -> int:
    paths = _paths()
    seen = set()
    for mod, table, _ in TRACKED:
        p = paths[mod]
        if p in seen:
            continue
        seen.add(p)
        s = fr.change_summary(p)
        print(f"\n{mod} ({p})")
        print("  changes logged:", format(s["total"], ","))
        if s["by_type"]:
            print("  by type :", s["by_type"])
        if s["by_table"]:
            for t, n in list(s["by_table"].items())[:8]:
                print(f"     {t:28} {n:,}")
        if s["top_fields"]:
            print("  busiest fields:",
                  ", ".join(f"{f}({n:,})" for f, n in s["top_fields"][:6]))
    return 0


def main() -> int:
    args = set(sys.argv[1:])
    if "--status" in args:
        return status()
    apply = "--apply" in args
    paths = _paths()

    print("=" * 66)
    print("content fingerprint backfill — offline, zero requests")
    print("=" * 66)
    t0 = time.time()
    tot_rows = tot_todo = tot_done = 0
    problems = []

    for mod, table, pk in TRACKED:
        path = paths[mod]
        if not _exists(path, table):
            print(f"  {table:30} — absent, skipped")
            continue
        missing = _pk_ok(path, table, pk)
        if missing:
            problems.append(f"{table}: no such column(s) {missing}")
            print(f"  {table:30} — PK COLUMN MISSING {missing}, skipped")
            continue
        try:
            r = fr.backfill(path, table, pk, apply=apply)
        except Exception as e:  # noqa: BLE001
            problems.append(f"{table}: {e}")
            print(f"  {table:30} — ERROR {e}")
            continue
        tot_rows += r["rows"]
        tot_todo += r["needing_hash"]
        tot_done += r["hashed"]
        print(f"  {table:30} {r['rows']:>9,} rows · "
              f"{r['needing_hash']:>9,} need a hash · "
              f"{r['columns_in_hash']:>3} cols hashed"
              + (f" · {r['hashed']:,} done" if apply else ""))

    print("-" * 66)
    print(f"  {'TOTAL':30} {tot_rows:>9,} rows · {tot_todo:>9,} need a hash"
          + (f" · {tot_done:,} hashed" if apply else ""))
    if problems:
        print("\n  problems:")
        for p in problems:
            print("   -", p)
    if not apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
    else:
        print(f"\ndone in {time.time()-t0:.1f}s — no rows deleted, no requests made.")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
