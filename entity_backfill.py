"""Label colleges vs universities — offline, zero requests.

Collegedunia's "colleges" listing mixes universities in with colleges. The only
thing distinguishing them is the URL shape: /university/<id>-slug against
/college/<id>-slug. That has been in `link` since the first crawl; nothing ever
recorded it, so a consumer of this data cannot tell the two apart.

This fills a new `entity_type` column on `colleges` and `colleges_directory`
from `link`. It makes no requests, deletes nothing, and only ever fills a blank
— a value already set is never overwritten, so re-running is a no-op.

    python entity_backfill.py            # dry run: report only, no writes
    python entity_backfill.py --apply    # perform the backfill
    python entity_backfill.py --verify   # post-hoc check + samples
"""
from __future__ import annotations

import sys

import db


def _fmt(n) -> str:
    return f"{n:,}" if isinstance(n, int) else str(n)


def report() -> dict:
    rep = db.entity_type_report()
    for tbl, r in rep.items():
        print(f"\n{tbl}")
        print(f"   rows total        : {_fmt(r['total'])}")
        print(f"   already typed     : {_fmt(r['already_typed'])}")
        print(f"   -> university     : {_fmt(r['university'])}")
        print(f"   -> college        : {_fmt(r['college'])}")
        print(f"   -> unknown (left NULL) : {_fmt(r['unknown'])}")
    return rep


def verify() -> None:
    with db.connect() as c:
        for tbl in db.ENTITY_TABLES:
            print(f"\n{tbl} — entity_type distribution")
            rows = c.execute(
                f"SELECT COALESCE(entity_type,'(null)') t, COUNT(*) n "
                f"FROM {tbl} GROUP BY t ORDER BY n DESC").fetchall()
            for t, n in rows:
                print(f"   {t:12} {n:>8,}")
            # A label must never contradict its own link.
            bad = c.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE "
                f"(entity_type='university' AND link NOT LIKE '%/university/%') OR "
                f"(entity_type='college'    AND link NOT LIKE '%/college/%')"
            ).fetchone()[0]
            print(f"   contradictions : {bad:,}" + ("  <-- BUG" if bad else "  ok"))
        print("\n   sample universities found in `colleges`:")
        for cid, name, link in c.execute(
                "SELECT college_id, name, link FROM colleges "
                "WHERE entity_type='university' LIMIT 5"):
            print(f"      {cid:>7}  {str(name)[:44]:46} {str(link)[:52]}")


def main() -> int:
    args = set(sys.argv[1:])
    db.init_db()          # adds the column if this is the first run

    if "--verify" in args:
        verify()
        return 0

    print("=" * 62)
    print("entity_type backfill — derived from `link`, zero requests")
    print("=" * 62)
    rep = report()

    total = sum(r["university"] + r["college"] for r in rep.values())
    if not total:
        print("\nNothing to do — every row with a usable link is already typed.")
        return 0

    if "--apply" not in args:
        print(f"\nDRY RUN — {_fmt(total)} rows would be labelled. "
              f"Re-run with --apply to write.")
        return 0

    print("\napplying...")
    changed = db.backfill_entity_type()
    for tbl, res in changed.items():
        print(f"   {tbl}: "
              + ", ".join(f"{v} {k}" for k, v in res.items() if v)
              or f"   {tbl}: no change")
    print("\nverifying...")
    verify()
    print("\ndone — no rows deleted, no requests made.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
