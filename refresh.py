"""The refresh queue — what has gone stale, and how to make a phase re-sweep it.

WHY THIS EXISTS
---------------
`freshness.py` records what changed; the upserts now call it. But nothing
decides WHAT to re-fetch or WHEN, so the fingerprints would sit there being
correct and unused. This is that decision, and only that: it reads the
freshness columns, reports the queue, and resets the progress bookkeeping so an
existing cheap listing phase runs again as a full sweep.

It never fetches anything itself. The verticals own their own scraping and are
better at it than a generic runner would be.

    python refresh.py                     # what is stale (read-only)
    python refresh.py --plan              # which sweep covers what, and its cost
    python refresh.py --reset cf_b        # dry run: what resetting would clear
    python refresh.py --reset cf_b --apply
    python refresh.py --finalize cf_offerings --since <epoch> --apply

THE TRAP THIS AVOIDS
--------------------
The obvious way to walk the college catalogue is `colleges_directory`. That is
wrong: the directory is NOT a superset of `colleges`. 453 enriched colleges —
IIT Bombay 25703, IIT Madras 25881, MIT Manipal 14265, AIIMS Jodhpur 25796 —
have no directory row at all, because they arrived through Phase 2 offerings
rather than the directory sweep. Anything that iterates the directory alone
silently never refreshes them. See
`claude/finding-college-discovery-topology-2026-09-18.md`.

SAFETY
------
Read-only unless --apply is passed, and even then it only ever DELETEs from
*progress* tables — cc_progress, dir_progress, offering_progress,
cf_course_progress, cf_partition_progress, sa_*_progress. Those are resume
bookkeeping, not scraped facts. No row of data is deleted or modified by this
module, which is what the standing rule requires.

`--finalize` is the one operation that writes to a data table, and it writes
only `inactive_since` — a flag, never a deletion. It refuses to run unless the
sweep it is finalising actually covered most of the table, because a run that
stopped early would otherwise flag everything it had not reached as vanished.
"""
from __future__ import annotations

import sys
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

import freshness as fr

DAY = 86400.0
DEFAULT_STALE_DAYS = 30.0

# A completed sweep must have re-seen at least this share of the table before
# --finalize will flag the remainder as gone. 0.80 is deliberately cautious:
# the cost of a false "gone" flag is a record that silently drops out of every
# downstream export, and the cost of not flagging is that it stays one more
# cycle.
FINALIZE_MIN_COVERAGE = 0.80


def _paths() -> Dict[str, str]:
    import db
    import sa_db
    import cf_db
    return {"db": db.DB_PATH, "sa_db": sa_db.SA_DB_PATH, "cf_db": cf_db.CF_DB_PATH}


# ---------------------------------------------------------------------------
# What a sweep covers, and what resetting it costs
#
# `unit_cost` figures are MEASURED, not assumed, and each one names where it
# came from. An unmeasured sweep carries None and prints "unmeasured" rather
# than a number — a made-up estimate is worse than no estimate, which this
# project has now demonstrated twice.
# ---------------------------------------------------------------------------
class Sweep:
    def __init__(self, key: str, label: str, mod: str, tables: Sequence[str],
                 progress: Sequence[str], unit: str,
                 unit_reqs: Optional[float] = None,
                 unit_kb: Optional[float] = None,
                 source: str = "", note: str = ""):
        self.key, self.label, self.mod = key, label, mod
        self.tables = list(tables)
        self.progress = list(progress)
        self.unit, self.unit_reqs, self.unit_kb = unit, unit_reqs, unit_kb
        self.source, self.note = source, note


SWEEPS: List[Sweep] = [
    Sweep("cf_a", "Course Finder Ⓐ — course catalogue", "cf_db",
          ["cf_courses"], ["cf_partition_progress"], "course",
          note="Partitions by course_tag_id, so it cannot see untagged "
               "courses. 654 of those were bridged in from domestic Phase 1 "
               "on 2026-09-18 and a reset does not re-discover them — they "
               "are already in cf_courses and stay there."),
    Sweep("cf_b", "Course Finder Ⓑ — offerings", "cf_db",
          ["cf_offerings"], ["cf_course_progress"], "course",
          unit_reqs=1.62, unit_kb=4.5,
          source="job 9, 2026-09-18: 656 courses, 1,062 requests, 2.9 MB"),
    Sweep("dom_dir", "domestic directory", "db",
          ["colleges_directory"], ["dir_progress"], "page"),
    Sweep("dom_cc", "domestic college courses", "db",
          ["college_courses"], ["cc_progress"], "college"),
    Sweep("sa_prog", "Study Abroad programme listing", "sa_db",
          ["sa_programs"], ["sa_program_progress"], "programme",
          unit_kb=2.5,
          source="listing API measured at ~2.5 KB/programme vs ~109 KB for "
                 "the detail page — 43x cheaper, which is the whole basis "
                 "for fingerprinting the listing instead of re-fetching "
                 "details"),
    Sweep("sa_univ", "Study Abroad universities", "sa_db",
          ["sa_universities"], ["sa_university_progress"], "university"),
]

BY_KEY = {s.key: s for s in SWEEPS}
SWEEP_FOR_TABLE = {t: s for s in SWEEPS for t in s.tables}


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
def table_state(conn, table: str, stale_days: float) -> Optional[Dict[str, Any]]:
    cutoff = time.time() - stale_days * DAY
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:  # noqa: BLE001
        return None
    if not fr.has_freshness(conn, table):
        return {"table": table, "total": total, "tracked": False}
    q = lambda w, a=(): conn.execute(  # noqa: E731
        f"SELECT COUNT(*) FROM {table} WHERE {w}", a).fetchone()[0]
    newest = conn.execute(
        f"SELECT MAX(last_seen_at) FROM {table}").fetchone()[0]
    return {
        "table": table, "total": total, "tracked": True,
        "stale": q("last_seen_at IS NULL OR last_seen_at < ?", (cutoff,)),
        "inactive": q("inactive_since IS NOT NULL"),
        "unhashed": q("content_hash IS NULL"),
        "last_seen": newest,
    }


def status(stale_days: float = DEFAULT_STALE_DAYS) -> Dict[str, Any]:
    import freshness_backfill as fb
    paths = _paths()
    out: List[Dict[str, Any]] = []
    for mod, table, _pk in fb.TRACKED:
        with fr.connect(paths[mod]) as conn:
            st = table_state(conn, table, stale_days)
        if st:
            st["mod"] = mod
            st["sweep"] = SWEEP_FOR_TABLE[table].key if table in SWEEP_FOR_TABLE else None
            out.append(st)
    return {"stale_days": stale_days, "tables": out}


def render_status(rep: Dict[str, Any]) -> str:
    L = ["=" * 74,
         f"REFRESH QUEUE — stale = not confirmed in "
         f"{rep['stale_days']:.0f} days (read-only)",
         "=" * 74, "",
         f"  {'table':<26}{'rows':>10}{'stale':>10}{'inactive':>10}"
         f"{'no hash':>9}  sweep"]
    warm, cold = [], []
    for t in rep["tables"]:
        if not t.get("tracked"):
            L.append(f"  {t['table']:<26}{t['total']:>10,}"
                     f"{'— not tracked; run freshness_backfill.py --apply':>40}")
            continue
        L.append(f"  {t['table']:<26}{t['total']:>10,}{t['stale']:>10,}"
                 f"{t['inactive']:>10,}{t['unhashed']:>9,}  "
                 f"{t['sweep'] or '—'}")
        (cold if t["stale"] else warm).append(t)
    L.append("")
    n_cold = sum(t["stale"] for t in cold)
    L.append(f"  {len(cold)} tables hold {n_cold:,} stale rows; "
             f"{len(warm)} tables are current.")
    covered = {t["table"] for t in cold if t["sweep"]}
    orphan = [t["table"] for t in cold if not t["sweep"]]
    if covered:
        keys = sorted({SWEEP_FOR_TABLE[t].key for t in covered})
        L.append(f"  refreshable by re-sweeping: {', '.join(keys)}")
    if orphan:
        L.append(f"  no cheap sweep refreshes these yet: {', '.join(orphan)}")
    return "\n".join(L)


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------
def plan(stale_days: float = DEFAULT_STALE_DAYS) -> str:
    rep = status(stale_days)
    by_table = {t["table"]: t for t in rep["tables"]}
    L = ["=" * 74, "REFRESH PLAN", "=" * 74]
    for s in SWEEPS:
        rows = [by_table[t] for t in s.tables if t in by_table]
        stale = sum(r.get("stale") or 0 for r in rows)
        total = sum(r.get("total") or 0 for r in rows)
        L.append("")
        L.append(f"  {s.key:<9} {s.label}")
        L.append(f"            tables: {', '.join(s.tables)}")
        L.append(f"            {total:,} rows, {stale:,} stale")
        if s.unit_reqs or s.unit_kb:
            bits = []
            if s.unit_reqs:
                bits.append(f"{s.unit_reqs:g} reqs/{s.unit}")
            if s.unit_kb:
                bits.append(f"{s.unit_kb:g} KB/{s.unit}")
            L.append(f"            measured: {', '.join(bits)}")
            L.append(f"            source:   {s.source}")
        else:
            L.append("            cost:     unmeasured — run it once and read "
                     "the job's own totals")
        if s.note:
            L.append(f"            note:     {s.note}")
        L.append(f"            reset:    python refresh.py --reset {s.key} --apply")
    L.append("")
    L.append("  A reset only clears resume bookkeeping. The next run of that")
    L.append("  phase then re-fetches everything and the upserts fingerprint")
    L.append("  what comes back, so only genuinely changed rows are logged.")
    return "\n".join(L)


# ---------------------------------------------------------------------------
# Reset — progress tables only
# ---------------------------------------------------------------------------
def reset(key: str, apply: bool = False) -> int:
    s = BY_KEY.get(key)
    if not s:
        print(f"unknown sweep {key!r}. known: {', '.join(sorted(BY_KEY))}")
        return 2
    path = _paths()[s.mod]
    print("=" * 74)
    print(f"reset {s.key} — {s.label}")
    print("=" * 74)
    with fr.connect(path) as conn:
        counts = {}
        for p in s.progress:
            try:
                counts[p] = conn.execute(f"SELECT COUNT(*) FROM {p}").fetchone()[0]
            except Exception:  # noqa: BLE001
                counts[p] = None
        for p, n in counts.items():
            print(f"  {p:<28} {'(absent)' if n is None else f'{n:,} rows'}")
        for t in s.tables:
            try:
                n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                print(f"  {t:<28} {n:,} data rows — NOT touched")
            except Exception:  # noqa: BLE001
                pass
        if not apply:
            print("\nDRY RUN — nothing cleared. Re-run with --apply.")
            return 0
        for p, n in counts.items():
            if n:
                conn.execute(f"DELETE FROM {p}")
        conn.commit()
    print(f"\ncleared. The next {s.label} run will sweep from the start.")
    print("Data rows were not touched; the upserts will fingerprint what "
          "comes back.")
    return 0


# ---------------------------------------------------------------------------
# Finalize — flag what a completed sweep did not see
# ---------------------------------------------------------------------------
def finalize(table: str, since: float, apply: bool = False,
             job_id: Optional[int] = None) -> int:
    import freshness_backfill as fb
    entry = next(((m, t, pk) for m, t, pk in fb.TRACKED if t == table), None)
    if not entry:
        print(f"{table!r} is not a tracked table.")
        return 2
    mod, _t, pk = entry
    path = _paths()[mod]
    with fr.connect(path) as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        seen = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE last_seen_at >= ?",
            (since,)).fetchone()[0]
        already = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE inactive_since IS NOT NULL"
        ).fetchone()[0]
        share = (seen / total) if total else 0.0
        print(f"  {table}: {total:,} rows, {seen:,} re-seen since the sweep "
              f"started ({share:.1%})")
        print(f"  would flag: {total - seen - already:,}   "
              f"already flagged: {already:,}")
        if share < FINALIZE_MIN_COVERAGE:
            print(f"\nREFUSED — the sweep only covered {share:.1%} of the "
                  f"table, under the {FINALIZE_MIN_COVERAGE:.0%} floor.")
            print("A partial sweep would flag everything it never reached as "
                  "vanished. Finish the sweep, then finalize.")
            return 1
        if not apply:
            print("\nDRY RUN — nothing flagged. Re-run with --apply.")
            return 0
        n = fr.mark_missing(conn, table, pk, since, job_id=job_id)
        conn.commit()
    print(f"\nflagged {n:,} rows inactive. None deleted — they keep their data "
          f"and can come back, which is logged as 'returned'.")
    return 0


# ---------------------------------------------------------------------------
def main(argv: Optional[Sequence[str]] = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    apply = "--apply" in args

    def val(flag: str) -> Optional[str]:
        return args[args.index(flag) + 1] if flag in args and \
            args.index(flag) + 1 < len(args) else None

    stale = float(val("--stale") or DEFAULT_STALE_DAYS)

    if "--reset" in args:
        return reset(val("--reset") or "", apply)
    if "--finalize" in args:
        since = val("--since")
        if not since:
            print("--finalize needs --since <epoch seconds>: the moment the "
                  "sweep started. Rows not re-seen since then are the ones "
                  "that vanished.")
            return 2
        jid = val("--job")
        return finalize(val("--finalize") or "", float(since), apply,
                        int(jid) if jid else None)
    if "--plan" in args:
        print(plan(stale))
        return 0
    print(render_status(status(stale)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
