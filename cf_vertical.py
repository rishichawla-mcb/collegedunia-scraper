"""
Registers the Course Finder vertical with the platform framework.

Self-contained: its own `cf_` tables, its own jobs/logs. It does not read or
write any domestic or Study Abroad table, and no other module reads its tables.
"""
from __future__ import annotations

BUILD = "2026-08-29a"

import vertical_base as vb
import cf_db
import cf_export
import cf_scraper


def _make_logger(job_id: int):
    def _log(msg: str):
        try:
            cf_db.add_log(job_id, msg)
        except Exception:  # noqa: BLE001
            pass
        print(f"[CF job {job_id}] {msg}", flush=True)
    return _log


COURSE_FINDER = vb.Vertical(
    name="coursefinder",
    label="🔎 Course Finder",
    description="collegedunia.com/course-finder — the full ~21.7k course catalogue "
                "and every college offering each course, with fees, ranking, cutoff "
                "and admission dates.",
    db_path=cf_db.CF_DB_PATH,
    init_db=cf_db.init_db,
    counts=cf_db.counts,
    export_xlsx=cf_export.to_xlsx,
    get_job=cf_db.get_job,
    make_logger=_make_logger,
    phases=[
        vb.Phase("catalogue", "Ⓐ Catalogue",
                 "Sweep the course-finder listing sliced by course_tag_id (the "
                 "unsliced listing caps at ~1,700 results). Captures every course "
                 "plus colleges_data.count — how many colleges offer it — which "
                 "costs nothing extra and lets phase B be forecast and prioritised.",
                 cf_scraper.run_catalogue),
        vb.Phase("offerings", "Ⓑ Offerings",
                 "For every catalogued course, page through "
                 "/course-finder?course_id=<id> to collect each college offering it, "
                 "with fees, ranking, cutoff, admission dates and rating. "
                 "Self-draining queue: interrupted courses resume automatically.",
                 cf_scraper.run_offerings, depends_on=["catalogue"]),
    ],
)

vb.register(COURSE_FINDER)
