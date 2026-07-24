"""
Registers the Study Abroad vertical with the platform framework.

This is the REFERENCE implementation every future vertical (MBBS Abroad, Rankings,
Exams, Scholarships, ...) should mirror: build a `Vertical` from your own db +
scraper + export modules and `register()` it. The framework/worker/UI then drive
it generically — no changes to the platform are needed to add a vertical.
"""
from __future__ import annotations

BUILD = "2026-07-23a"

import vertical_base as vb
import sa_db
import sa_scraper
import sa_export


def _make_logger(job_id: int):
    def _log(msg: str):
        try:
            sa_db.add_log(job_id, msg)
        except Exception:  # noqa: BLE001
            pass
        print(f"[SA job {job_id}] {msg}", flush=True)
    return _log


STUDY_ABROAD = vb.Vertical(
    name="studyabroad",
    label="🌍 Study Abroad",
    description="Collegedunia Study Abroad course-finder (107k+ programs, 31 countries).",
    db_path=sa_db.SA_DB_PATH,
    init_db=sa_db.init_db,
    counts=sa_db.counts,
    export_xlsx=sa_export.to_xlsx,
    get_job=sa_db.get_job,
    make_logger=_make_logger,
    phases=[
        vb.Phase("facets", "① Facets",
                 "Capture all filter facets (country/level/stream/…) with counts from the "
                 "course-finder SSR data. Feeds the partition planner.",
                 sa_scraper.run_facets),
        vb.Phase("programs", "② Programs",
                 "Partitioned crawl of ALL programs via listing-cf-sa (country→course_type→"
                 "stream until each slice < ~10k cap). Captures max fields incl. currency and "
                 "program_url; derives universities + countries + exam requirements.",
                 sa_scraper.run_programs, depends_on=["facets"]),
        # Future phases slot in here: program_details, university_enrich, scholarships,
        # verification — each a Phase with its own runner. No framework change needed.
    ],
)

vb.register(STUDY_ABROAD)
