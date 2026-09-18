"""Coverage and freshness report — read-only, zero requests.

WHY THIS EXISTS
---------------
Two large gaps hid in this dataset for months, and both were found by accident:

  * domestic `offerings` covered 59 of 16,896 courses (0.35%) while looking
    healthy at 84,553 rows — the row count concealed the shape
  * domestic Phase 1 had discovered 654 courses that the Course Finder
    catalogue never saw, so phase Ⓑ had never fetched their colleges

Nothing in the platform compared one source against another, so nothing could
have surfaced either. This does. It answers, without making a single request:

  what has each source discovered, and what does only one source know?
  what has been discovered but never enriched?
  what has not been confirmed by a sweep recently?
  what has disappeared from the site?

The last two need the columns `freshness.py` adds; this degrades gracefully and
says so when they are absent.

    python coverage_report.py              # full report
    python coverage_report.py --json       # machine-readable
    python coverage_report.py --stale 30   # freshness horizon in days
"""
from __future__ import annotations

BUILD = "2026-09-18a"

import json
import sqlite3
import sys
import time
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Safe access — every query here must tolerate a missing table or column, so a
# report can never be the thing that breaks.
# ---------------------------------------------------------------------------
def _conn(path: str) -> sqlite3.Connection:
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def _scalar(conn: sqlite3.Connection, sql: str, args=()) -> Optional[int]:
    try:
        return conn.execute(sql, args).fetchone()[0]
    except Exception:  # noqa: BLE001
        return None


def _ids(conn: sqlite3.Connection, sql: str) -> Optional[set]:
    try:
        return {r[0] for r in conn.execute(sql) if r[0] is not None}
    except Exception:  # noqa: BLE001
        return None


def _has_col(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        return col in {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    except Exception:  # noqa: BLE001
        return False


def _fmt(n) -> str:
    return "—" if n is None else f"{n:,}"


def _pct(a, b) -> str:
    if not a or not b:
        return "—"
    return f"{100.0 * a / b:.1f}%"


# ---------------------------------------------------------------------------
# 1. Cross-source discovery — what does only ONE source know about?
# ---------------------------------------------------------------------------
def cross_source(paths: Dict[str, str]) -> List[Dict[str, Any]]:
    out = []
    d = _conn(paths["db"])
    c = _conn(paths["cf_db"])
    try:
        pairs = [
            ("courses", "domestic Phase 1", d, "SELECT course_id FROM courses",
             "Course Finder Ⓐ", c, "SELECT course_id FROM cf_courses"),
            ("colleges", "domestic colleges", d, "SELECT college_id FROM colleges",
             "CF offerings", c, "SELECT DISTINCT college_id FROM cf_offerings"),
            ("colleges", "domestic directory", d,
             "SELECT college_id FROM colleges_directory",
             "CF offerings", c, "SELECT DISTINCT college_id FROM cf_offerings"),
            # The directory was assumed to be the discovery layer and `colleges`
            # its enriched subset. It is not: 453 enriched colleges — IIT Bombay
            # 25703, IIT Madras 25881, MIT Manipal 14265, AIIMS Jodhpur 25796 —
            # have no directory row at all. They came in via Phase 2 offerings.
            # Anything that walks the directory to decide what to refresh will
            # therefore silently skip them, which is why this pair is reported.
            ("colleges", "domestic directory", d,
             "SELECT college_id FROM colleges_directory",
             "domestic colleges", d, "SELECT college_id FROM colleges"),
        ]
        for entity, an, ac, aq, bn, bc, bq in pairs:
            A, B = _ids(ac, aq), _ids(bc, bq)
            if A is None or B is None:
                continue
            out.append({
                "entity": entity, "a_name": an, "b_name": bn,
                "a": len(A), "b": len(B), "both": len(A & B),
                "a_only": len(A - B), "b_only": len(B - A),
                "union": len(A | B),
                "a_only_sample": sorted(A - B)[:5],
                "b_only_sample": sorted(B - A)[:5],
            })
    finally:
        d.close(); c.close()
    return out


# ---------------------------------------------------------------------------
# 2. Discovered but never enriched — the work the platform knows about and has
#    not done. This is where the 5,309 directory colleges would have shown up.
# ---------------------------------------------------------------------------
def unenriched(paths: Dict[str, str]) -> List[Dict[str, Any]]:
    out = []
    d = _conn(paths["db"])
    s = _conn(paths["sa_db"])
    c = _conn(paths["cf_db"])
    try:
        rows = [
            # Deliberately named for what it measures. It is NOT "colleges we
            # have not enriched": the directory is not a superset of `colleges`
            # (see the last pair in section 1), so 453 enriched colleges are
            # outside this denominator entirely.
            ("directory rows with no colleges row", d,
             "SELECT COUNT(*) FROM colleges_directory",
             "SELECT COUNT(*) FROM colleges_directory x WHERE NOT EXISTS "
             "(SELECT 1 FROM colleges y WHERE y.college_id=x.college_id)"),
            ("colleges without Phase-3 enrichment", d,
             "SELECT COUNT(*) FROM colleges",
             "SELECT COUNT(*) FROM colleges WHERE enriched_at IS NULL"),
            ("CF courses without offerings (phase Ⓑ queue)", c,
             "SELECT COUNT(*) FROM cf_courses",
             "SELECT COUNT(*) FROM cf_courses x LEFT JOIN cf_course_progress p "
             "ON p.course_id=x.course_id WHERE p.course_id IS NULL "
             "OR p.status NOT IN ('done','empty')"),
            ("SA universities without detail", s,
             "SELECT COUNT(*) FROM sa_universities",
             "SELECT COUNT(*) FROM sa_universities WHERE detail_scraped_at IS NULL"),
            ("SA programmes without detail", s,
             "SELECT COUNT(*) FROM sa_programs",
             "SELECT COUNT(*) FROM sa_programs WHERE program_detail_scraped_at IS NULL"),
            ("SA exam rows still missing out_of", s,
             "SELECT COUNT(*) FROM sa_program_exams",
             "SELECT COUNT(*) FROM sa_program_exams WHERE COALESCE(out_of,'')=''"),
            ("domestic course rows without course_url", d,
             "SELECT COUNT(*) FROM college_courses",
             "SELECT COUNT(*) FROM college_courses WHERE COALESCE(course_url,'')=''"),
        ]
        for label, conn, total_q, pending_q in rows:
            tot, pend = _scalar(conn, total_q), _scalar(conn, pending_q)
            if tot is None:
                continue
            out.append({"label": label, "total": tot, "pending": pend,
                        "done": None if pend is None else tot - pend})
    finally:
        d.close(); s.close(); c.close()
    return out


# ---------------------------------------------------------------------------
# 3. Freshness — needs freshness.py's columns
# ---------------------------------------------------------------------------
FRESHNESS_TABLES = [
    ("db", "colleges"), ("db", "courses"), ("db", "college_courses"),
    ("db", "offerings"), ("db", "colleges_directory"),
    ("sa_db", "sa_programs"), ("sa_db", "sa_universities"),
    ("sa_db", "sa_countries"), ("sa_db", "sa_program_exams"),
    ("sa_db", "sa_scholarships"), ("sa_db", "sa_university_rankings"),
    ("sa_db", "sa_university_courses"), ("sa_db", "sa_program_fees"),
    ("sa_db", "sa_program_scholarships"),
    ("cf_db", "cf_courses"), ("cf_db", "cf_offerings"),
]


def freshness(paths: Dict[str, str], stale_days: float = 30.0) -> Dict[str, Any]:
    now = time.time()
    cutoff = now - stale_days * 86400
    rows, tracked = [], 0
    conns = {k: _conn(v) for k, v in paths.items()}
    try:
        for mod, table in FRESHNESS_TABLES:
            conn = conns[mod]
            total = _scalar(conn, f"SELECT COUNT(*) FROM {table}")
            if total is None:
                continue
            if not _has_col(conn, table, "last_seen_at"):
                rows.append({"table": table, "total": total, "tracked": False})
                continue
            tracked += 1
            rows.append({
                "table": table, "total": total, "tracked": True,
                "never_seen": _scalar(conn, f"SELECT COUNT(*) FROM {table} "
                                            f"WHERE last_seen_at IS NULL"),
                "stale": _scalar(conn, f"SELECT COUNT(*) FROM {table} "
                                       f"WHERE last_seen_at < ?", (cutoff,)),
                "inactive": _scalar(conn, f"SELECT COUNT(*) FROM {table} "
                                          f"WHERE inactive_since IS NOT NULL"),
                "hashed": _scalar(conn, f"SELECT COUNT(*) FROM {table} "
                                        f"WHERE content_hash IS NOT NULL"),
                # `> first_seen_at` is load-bearing. freshness.backfill() seeds
                # last_changed_at from the row's own scraped_at, so without this
                # clause every row scraped inside the horizon reads as "changed"
                # even though nothing has ever been compared against anything.
                # The first live run of this report showed 337,571 cf_offerings
                # "changed" while the change log (section 4) said zero — the log
                # was right. A row only counts as changed once an observe() call
                # has actually moved its hash after it was first recorded.
                "changed_recently": _scalar(
                    conn, f"SELECT COUNT(*) FROM {table} WHERE last_changed_at >= ? "
                          f"AND last_changed_at > COALESCE(first_seen_at, 0)",
                    (cutoff,)),
            })
    finally:
        for c in conns.values():
            c.close()
    return {"stale_days": stale_days, "tables": rows,
            "tracked_tables": tracked, "total_tables": len(rows)}


def change_activity(paths: Dict[str, str], days: float = 30.0) -> Dict[str, Any]:
    since = time.time() - days * 86400
    out: Dict[str, Any] = {"days": days, "by_db": {}}
    for mod, path in paths.items():
        conn = _conn(path)
        try:
            n = _scalar(conn, "SELECT COUNT(*) FROM data_changes WHERE changed_at>=?",
                        (since,))
            if n is None:
                continue
            types = {}
            try:
                types = {r[0]: r[1] for r in conn.execute(
                    "SELECT change_type,COUNT(*) FROM data_changes "
                    "WHERE changed_at>=? GROUP BY change_type", (since,))}
            except Exception:  # noqa: BLE001
                pass
            out["by_db"][mod] = {"total": n, "by_type": types}
        finally:
            conn.close()
    return out


# ---------------------------------------------------------------------------
def build(stale_days: float = 30.0) -> Dict[str, Any]:
    import db
    import sa_db
    import cf_db
    paths = {"db": db.DB_PATH, "sa_db": sa_db.SA_DB_PATH, "cf_db": cf_db.CF_DB_PATH}
    return {
        "generated_at": time.time(),
        "cross_source": cross_source(paths),
        "unenriched": unenriched(paths),
        "freshness": freshness(paths, stale_days),
        "changes": change_activity(paths, stale_days),
    }


def render(rep: Dict[str, Any]) -> str:
    L = []
    L.append("=" * 72)
    L.append("COVERAGE & FRESHNESS REPORT — read-only, no requests")
    L.append("=" * 72)

    L.append("\n1. CROSS-SOURCE DISCOVERY")
    L.append("   Ids only one source knows about are coverage you are missing.")
    for x in rep["cross_source"]:
        L.append(f"\n   {x['entity']}:  {x['a_name']}  vs  {x['b_name']}")
        L.append(f"     {x['a_name']:<22} {_fmt(x['a']):>9}")
        L.append(f"     {x['b_name']:<22} {_fmt(x['b']):>9}")
        L.append(f"     {'in both':<22} {_fmt(x['both']):>9}")
        flag = "   <-- gap" if x["a_only"] else ""
        L.append(f"     {'ONLY ' + x['a_name']:<22} {_fmt(x['a_only']):>9}{flag}")
        flag = "   <-- gap" if x["b_only"] else ""
        L.append(f"     {'ONLY ' + x['b_name']:<22} {_fmt(x['b_only']):>9}{flag}")
        L.append(f"     {'union (true total)':<22} {_fmt(x['union']):>9}")
        if x["a_only"]:
            L.append(f"       sample: {x['a_only_sample']}")
        if x["b_only"]:
            L.append(f"       sample: {x['b_only_sample']}")

    L.append("\n2. DISCOVERED BUT NOT ENRICHED")
    L.append(f"   {'':<46}{'total':>10}{'pending':>10}{'done':>8}")
    for x in rep["unenriched"]:
        L.append(f"   {x['label']:<46}{_fmt(x['total']):>10}"
                 f"{_fmt(x['pending']):>10}{_pct(x['done'], x['total']):>8}")

    f = rep["freshness"]
    L.append(f"\n3. FRESHNESS  (stale = not confirmed in {f['stale_days']:.0f} days)")
    if not f["tracked_tables"]:
        L.append("   No table carries freshness columns yet.")
        L.append("   Run:  python freshness_backfill.py --apply")
    else:
        L.append(f"   {'table':<24}{'rows':>9}{'hashed':>9}{'stale':>9}"
                 f"{'inactive':>10}{'changed':>9}")
        for t in f["tables"]:
            if not t.get("tracked"):
                L.append(f"   {t['table']:<24}{_fmt(t['total']):>9}"
                         f"{'not tracked':>37}")
                continue
            L.append(f"   {t['table']:<24}{_fmt(t['total']):>9}"
                     f"{_fmt(t['hashed']):>9}{_fmt(t['stale']):>9}"
                     f"{_fmt(t['inactive']):>10}{_fmt(t['changed_recently']):>9}")

    ch = rep["changes"]
    L.append(f"\n4. CHANGE ACTIVITY  (last {ch['days']:.0f} days)")
    if not ch["by_db"]:
        L.append("   No change log yet — it fills as refresh sweeps run.")
    else:
        for mod, v in ch["by_db"].items():
            L.append(f"   {mod:<10} {_fmt(v['total']):>9} changes   {v['by_type']}")

    L.append("")
    return "\n".join(L)


def main() -> int:
    args = sys.argv[1:]
    stale = 30.0
    if "--stale" in args:
        try:
            stale = float(args[args.index("--stale") + 1])
        except (IndexError, ValueError):
            pass
    rep = build(stale)
    if "--json" in args:
        print(json.dumps(rep, indent=1, default=str))
    else:
        print(render(rep))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
