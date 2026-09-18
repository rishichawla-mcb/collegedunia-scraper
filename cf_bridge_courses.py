"""Bridge courses discovered by domestic Phase 1 into the Course Finder catalogue.

WHY
---
The two catalogue sweeps partition the listing differently, so each reaches
courses the other cannot:

  domestic Phase 1 partitions by stream        -> 16,896 courses
  Course Finder  Ⓐ partitions by course_tag_id -> 16,243 courses
  in both 16,242 · domestic only 654 · CF only 1

The 654 are real (all named, all linked) and they are the long tail — "Advanced
Diploma in Croatian", "…in Slovak" — courses with no course_tag_id, which is
exactly why a tag-facet sweep cannot see them. Because they are absent from
cf_courses, Course Finder Ⓑ has never fetched offerings for them: at the
measured ~28 colleges per course that is roughly 18,000 offerings missing from
the best table in the dataset.

Copying the ids across costs ZERO requests — the rows are already on disk. Once
they are in cf_courses, Ⓑ's self-draining queue picks them up on its next run.

    python cf_bridge_courses.py            # report only, no writes
    python cf_bridge_courses.py --apply    # copy them across
    python cf_bridge_courses.py --status   # how the bridged courses are doing

SAFETY
------
Only INSERTs courses absent from cf_courses. An existing cf_courses row is never
touched, because the CF sweep's own data is better than a cross-vertical copy:
it carries colleges_count and course_tag_id, which this does not. Nothing is
deleted.

Bridged rows are stamped source_job_id = BRIDGE_JOB_ID (-1) so they stay
identifiable — you can always tell what came from the sweep and what was
imported, and undo this cleanly if it turns out to be a mistake.
"""
from __future__ import annotations

import sys
import time

BRIDGE_JOB_ID = -1

# domestic `courses` column -> cf_courses column. Columns present in one schema
# and not the other are simply not carried: `program_type`, `mode`,
# `stream_name` and `colleges_url` have no cf_courses home, and cf_courses'
# `course_could_be` / `degree_could_be` / `listing_link` have no domestic
# source. colleges_count is deliberately NOT copied — see below.
COLUMN_MAP = {
    "course_id": "course_id",
    "name": "name",
    "course_link": "course_link",
    "listing_link": "listing_link",
    "description": "description",
    "eligibility": "eligibility",
    "duration": "duration",
    "level": "level",
    "course_type": "course_type",
    "fees": "fees",
    "avg_salary": "avg_salary",
    "exam_name": "exam_name",
    "exam_url": "exam_url",
    "job_roles": "job_roles",
    "topics_covered": "topics_covered",
    "stream_id": "stream_id",
    "course_tag": "course_tag",
    "course_tag_id": "course_tag_id",
    "raw_json": "raw_json",
}


def _missing():
    """(rows to bridge, domestic total, cf total) — reads only."""
    import db
    import cf_db
    with db.connect(db.DB_PATH) as d:
        dom = {r["course_id"]: dict(r) for r in d.execute("SELECT * FROM courses")}
    with cf_db.connect(cf_db.CF_DB_PATH) as c:
        cf = {r[0] for r in c.execute("SELECT course_id FROM cf_courses")}
    missing = [row for cid, row in dom.items() if cid not in cf]
    missing.sort(key=lambda r: r["course_id"])
    return missing, len(dom), len(cf)


def status() -> int:
    import cf_db
    with cf_db.connect(cf_db.CF_DB_PATH) as c:
        n = c.execute("SELECT COUNT(*) FROM cf_courses WHERE source_job_id=?",
                      (BRIDGE_JOB_ID,)).fetchone()[0]
        if not n:
            print("no bridged courses in cf_courses yet.")
            return 0
        done = c.execute(
            "SELECT COUNT(*) FROM cf_courses c JOIN cf_course_progress p "
            "ON p.course_id=c.course_id WHERE c.source_job_id=? "
            "AND p.status IN ('done','empty')", (BRIDGE_JOB_ID,)).fetchone()[0]
        offers = c.execute(
            "SELECT COUNT(*) FROM cf_offerings o WHERE o.course_id IN "
            "(SELECT course_id FROM cf_courses WHERE source_job_id=?)",
            (BRIDGE_JOB_ID,)).fetchone()[0]
    print(f"  bridged courses      : {n:,}")
    print(f"  phase Ⓑ complete     : {done:,}")
    print(f"  still queued         : {n - done:,}")
    print(f"  offerings from them  : {offers:,}")
    return 0


def main() -> int:
    args = set(sys.argv[1:])
    import cf_db
    cf_db.init_db()
    if "--status" in args:
        return status()

    apply = "--apply" in args
    missing, n_dom, n_cf = _missing()

    print("=" * 64)
    print("bridge domestic-only courses into the Course Finder catalogue")
    print("=" * 64)
    print(f"  domestic courses : {n_dom:,}")
    print(f"  cf_courses       : {n_cf:,}")
    print(f"  to bridge        : {len(missing):,}")
    if not missing:
        print("\nnothing to do — cf_courses already covers every domestic course.")
        return 0

    named = sum(1 for r in missing if (r.get("name") or "").strip())
    linked = sum(1 for r in missing if (r.get("course_link") or "").strip())
    print(f"  of which named   : {named:,}")
    print(f"  of which linked  : {linked:,}")
    print("\n  sample:")
    for r in missing[:6]:
        print(f"    {r['course_id']:>7}  {str(r.get('name'))[:46]:48}"
              f"{str(r.get('stream_name') or '')[:16]}")

    # ~28 colleges per course measured across the catalogue; each course costs
    # roughly ceil(28/10)+1 listing requests in phase Ⓑ.
    print(f"\n  phase Ⓑ would then fetch ~{len(missing) * 28:,} offerings "
          f"in ~{len(missing) * 4:,} requests")

    if not apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return 0

    cols = list(COLUMN_MAP.values())
    ph = ",".join("?" for _ in cols) + ",?,?"
    sql = (f"INSERT OR IGNORE INTO cf_courses "
           f"({','.join(cols)},scraped_at,source_job_id) VALUES({ph})")
    now = time.time()
    rows = [tuple(r.get(src) for src in COLUMN_MAP) + (now, BRIDGE_JOB_ID)
            for r in missing]
    with cf_db.connect(cf_db.CF_DB_PATH) as c:
        before = c.execute("SELECT COUNT(*) FROM cf_courses").fetchone()[0]
        c.executemany(sql, rows)
        after = c.execute("SELECT COUNT(*) FROM cf_courses").fetchone()[0]

    print(f"\n  cf_courses {before:,} -> {after:,}  (+{after-before:,})")
    pend = len(cf_db.courses_pending())
    print(f"  phase Ⓑ queue is now {pend:,} courses")
    print("\ndone — no requests made, no existing row modified.")
    print("Run Course Finder -> Ⓑ Offerings to collect their colleges.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
