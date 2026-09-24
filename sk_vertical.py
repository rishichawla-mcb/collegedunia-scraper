"""
Registers the Shiksha vertical with the platform framework.

Fully isolated: its own SQLite FILE (not just its own prefix), its own jobs and
logs. It reads and writes nothing belonging to the domestic, Study Abroad or
Course Finder verticals, and nothing else reads its tables. Owner instruction,
2026-09-24: *"we will keep both db separate"*.
"""
from __future__ import annotations

BUILD = "2026-09-24a"

import vertical_base as vb
import sk_db
import sk_scraper


def _make_logger(job_id: int):
    def _log(msg: str):
        try:
            sk_db.add_log(job_id, msg)
        except Exception:  # noqa: BLE001
            pass
        print(f"[SK job {job_id}] {msg}", flush=True)
    return _log


SHIKSHA = vb.Vertical(
    name="shiksha",
    label="📗 Shiksha",
    description="shiksha.com — 57,695 colleges, discovered from the site's own "
                "sitemaps (ids are in the URLs, so discovery costs ~48 requests "
                "and every college × course edge is free). Its own database file.",
    db_path=sk_db.SK_DB_PATH,
    init_db=sk_db.init_db,
    counts=sk_db.counts,
    get_job=sk_db.get_job,
    make_logger=_make_logger,
    list_jobs=sk_db.list_jobs,      # orphan recovery (vb.reap_stale_jobs)
    update_job=sk_db.update_job,
    phases=[
        vb.Phase("discovery", "Ⓐ Discovery",
                 "Read the college, university and listing sitemaps. Builds the "
                 "full college inventory deduplicated BY ID (84,193 home URLs "
                 "resolve to 57,695 ids — the ~26k alias slugs are kept, not "
                 "discarded), every university, and every college × course "
                 "offering edge, all from URLs alone. ~48 gzipped requests, no "
                 "college page fetched. Resumable per sitemap.",
                 sk_scraper.run_discovery),
    ],
)

vb.register(SHIKSHA)
