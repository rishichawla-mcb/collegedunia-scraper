"""
Offline re-extraction — recover fields from raw_json that were never parsed.

NO NETWORK. NO DELETES. Every scraped row keeps the untouched API payload in
raw_json, so fields the original parser missed can be recovered without issuing
a single request. Useful in its own right, and essential while the scraper is
being 403-blocked: the data is already on disk.

What it recovers, per directory college:
  * top_course_fees  — the parser probed 'fees'/'amount'/'value'/'total_fees'
                       but the API sends 'fee'/'fee_formatted', so this column
                       was 0% filled on every row.
  * placement (avg + highest salary, placement %), facilities,
    major_stream_rating, media counts, tagline, logo/cover URLs.
    Reviews are deliberately NOT extracted or stored.
  * availableTabs   — which sub-pages exist per college (admission, placement,
                      scholarship, hostel, faculty, news, ranking...). This is
                      the discovery list for future modules.
  * rankingData[]   — real NIRF / India Today / Collegedunia ranking records,
                      written to the college_rankings table.

Writes are FILL-EMPTY-ONLY: a column that already has a value is left alone.

    python reparse.py                # re-extract directory rows
    python reparse.py --dry-run      # report what WOULD be filled, change nothing
    python reparse.py --limit 500
"""
from __future__ import annotations

BUILD = "2026-07-23a"

import argparse
import json
import time
from typing import Any, Dict, Iterator, List

import db
import scraper

CHUNK = 2000


def _iter_directory_raw(db_path: str, limit: int | None) -> Iterator[tuple]:
    sql = ("SELECT college_id, raw_json FROM colleges_directory "
           "WHERE raw_json IS NOT NULL AND raw_json<>'' ORDER BY college_id")
    if limit:
        sql += f" LIMIT {int(limit)}"
    with db.connect(db_path) as conn:
        for row in conn.execute(sql):
            yield row["college_id"], row["raw_json"]


def reparse_directory(db_path: str = None, limit: int | None = None,
                      dry_run: bool = False,
                      log=lambda m: print(m, flush=True)) -> Dict[str, int]:
    db_path = db_path or db.DB_PATH
    db.init_db(db_path)                       # ensure the new columns exist

    stats = {"scanned": 0, "unparseable": 0, "updated": 0,
             "ranking_rows": 0, "ranking_colleges": 0}
    would_fill: Dict[str, int] = {}
    batch: List[Dict[str, Any]] = []
    ranks: List[Dict[str, Any]] = []
    t0 = time.time()

    def flush():
        if dry_run:
            batch.clear(); ranks.clear()
            return
        if batch:
            stats["updated"] += db.fill_empty_directory_extras(batch, db_path=db_path)
            batch.clear()
        if ranks:
            db.upsert_college_rankings(ranks, db_path=db_path)
            ranks.clear()

    for cid, rj in _iter_directory_raw(db_path, limit):
        stats["scanned"] += 1
        try:
            obj = json.loads(rj)
        except Exception:  # noqa: BLE001
            stats["unparseable"] += 1
            continue
        if not isinstance(obj, dict):
            stats["unparseable"] += 1
            continue
        extras = scraper.parse_directory_extras(obj)
        extras["college_id"] = cid
        for k, v in extras.items():
            if k != "college_id" and v not in (None, "", 0):
                would_fill[k] = would_fill.get(k, 0) + 1
        batch.append(extras)

        rr = scraper.parse_directory_rankings(obj)
        if rr:
            stats["ranking_colleges"] += 1
            stats["ranking_rows"] += len(rr)
            ranks.extend(rr)

        if len(batch) >= CHUNK:
            flush()
            log(f"  … {stats['scanned']:,} scanned")
    flush()

    log("")
    log(f"{'DRY RUN — nothing written' if dry_run else 'done'} "
        f"in {time.time()-t0:.1f}s")
    log(f"  scanned      : {stats['scanned']:,}")
    log(f"  unparseable  : {stats['unparseable']:,}")
    if not dry_run:
        log(f"  rows updated : {stats['updated']:,}  (fill-empty-only)")
    log(f"  rankings     : {stats['ranking_rows']:,} rows across "
        f"{stats['ranking_colleges']:,} colleges")
    log("")
    log(f"  {'field':28} {'available':>10}")
    log("  " + "-" * 40)
    for k in sorted(would_fill, key=lambda x: -would_fill[x]):
        log(f"  {k:28} {would_fill[k]:>10,}")
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description="Offline re-extraction from raw_json")
    ap.add_argument("--db", default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be filled; write nothing")
    args = ap.parse_args()
    path = args.db or db.DB_PATH
    print(f"DB: {path}")
    reparse_directory(path, limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
