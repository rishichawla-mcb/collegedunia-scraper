"""Repair `colleges.nearest_airport` from the already-stored `basic_info_json`.

The Phase-3 parser stringified the whole airport dict instead of taking its
`name`, so rows read like  "{'name': 'Helipad', 'lat': 12.96, ...}".
This re-extracts the name and distance from data that is ALREADY in the DB.

  * zero network requests
  * nothing is deleted: the full dict stays in basic_info_json, and a row is
    only touched when a real name can be recovered from it
  * safe to re-run; already-clean rows are skipped

Run from the app directory:  python fix_airport.py          (report only)
                             python fix_airport.py --apply   (write)
"""
import ast
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db  # noqa: E402


def _to_int(v):
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None


def _airport(bi):
    """Return (name, distance_m) from a basic_info dict, or (None, None)."""
    if not isinstance(bi, dict):
        return None, None
    addr = bi.get("address")
    if not isinstance(addr, dict):
        return None, None
    a = addr.get("nearest_airport")
    if isinstance(a, dict):
        name = str(a.get("name") or "").strip()
        return (name or None), _to_int(a.get("distance"))
    if isinstance(a, str) and a.strip() and not a.strip().startswith(("{", "[")):
        return a.strip(), None
    return None, None


def _load(blob):
    if not blob:
        return None
    try:
        return json.loads(blob)
    except Exception:
        pass
    try:                                   # tolerate a repr() that got stored
        return ast.literal_eval(blob)
    except Exception:
        return None


def main(apply_changes=False, db_path=None):
    db_path = db_path or db.DB_PATH
    print(f"DB: {db_path}   mode: {'APPLY' if apply_changes else 'REPORT ONLY'}\n")

    with db.connect(db_path) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(colleges)")}
        if "nearest_airport_distance_m" not in cols:
            if apply_changes:
                conn.execute("ALTER TABLE colleges ADD COLUMN "
                             "nearest_airport_distance_m INTEGER")
                print("added column nearest_airport_distance_m")
            else:
                print("NOTE: column nearest_airport_distance_m is missing "
                      "(--apply will add it)")

        rows = conn.execute(
            "SELECT college_id, nearest_airport, basic_info_json FROM colleges "
            "WHERE basic_info_json IS NOT NULL AND basic_info_json<>''"
        ).fetchall()

        scanned = broken = repaired = unrecoverable = already_ok = 0
        samples = []
        updates = []

        for r in rows:
            scanned += 1
            cur = (r["nearest_airport"] or "").strip()
            name, dist = _airport(_load(r["basic_info_json"]))
            looks_broken = cur.startswith("{") or cur.startswith("[")
            if looks_broken:
                broken += 1
            if not name:
                if looks_broken:
                    unrecoverable += 1
                continue
            if cur == name and not looks_broken:
                already_ok += 1
                if dist is None:
                    continue
            updates.append((name, dist, r["college_id"]))
            if looks_broken and len(samples) < 5:
                samples.append((r["college_id"], cur[:56], name, dist))

        print(f"rows with basic_info_json : {scanned:,}")
        print(f"  stored as a raw dict    : {broken:,}")
        print(f"  recoverable             : {broken - unrecoverable:,}")
        print(f"  unrecoverable           : {unrecoverable:,}")
        print(f"  rows to write           : {len(updates):,}")

        if samples:
            print("\nsample repairs:")
            for cid, before, after, dist in samples:
                print(f"  [{cid}] {before}...")
                print(f"      -> {after!r}  distance_m={dist}")

        if not apply_changes:
            print("\nnothing written. re-run with --apply to commit.")
            return 0

        conn.executemany(
            "UPDATE colleges SET nearest_airport=?, "
            "nearest_airport_distance_m=COALESCE(?, nearest_airport_distance_m) "
            "WHERE college_id=?", updates)
        repaired = conn.total_changes
        conn.commit()
        print(f"\nwrote {len(updates):,} rows.")

        left = conn.execute(
            "SELECT COUNT(*) FROM colleges WHERE nearest_airport LIKE '{%'"
        ).fetchone()[0]
        filled = conn.execute(
            "SELECT COUNT(*) FROM colleges WHERE COALESCE(nearest_airport,'')<>''"
        ).fetchone()[0]
        print(f"nearest_airport filled   : {filled:,}")
        print(f"still stored as raw dict : {left:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main("--apply" in sys.argv))
