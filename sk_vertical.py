"""
Registers the Shiksha vertical with the platform framework.

Fully isolated: its own SQLite FILE (not just its own prefix), its own jobs and
logs. It reads and writes nothing belonging to the domestic, Study Abroad or
Course Finder verticals, and nothing else reads its tables. Owner instruction,
2026-09-24: *"we will keep both db separate"*.
"""
from __future__ import annotations

BUILD = "2026-09-28a"

import vertical_base as vb
import sk_courses
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
        vb.Phase("detail", "Ⓑ College detail",
                 "One request per college home page. The whole dataset is in the "
                 "page (__PRELOADED_STATE__, 616 KB), so there is no cheaper "
                 "endpoint; ~2.3 KB per college survives parsing, which is what "
                 "makes 57,751 colleges fit on the disk. Fetches DIRECT by "
                 "default and switches to the proxy permanently on the first "
                 "site refusal. Builds the shared base-course catalogue — the "
                 "join key the Collegedunia comparison needs.",
                 sk_scraper.run_detail, depends_on=["discovery"]),
        vb.Phase("courses", "Ⓒ Course listing",
                 "The per-course rows phase Ⓑ cannot give. Ⓑ reads "
                 "`baseCourseTuples`, which are grouped BY BASE COURSE — one "
                 "'B.Des' row with a fee RANGE covering every B.Des the college "
                 "runs. This walks /courses and /courses-2… (12 courses a page, "
                 "the site's own paginationData) and writes one row per ACTUAL "
                 "course: fees as a number, duration, seats, exams, "
                 "eligibility, median salary, skills, admission status — plus "
                 "the per-course URL phase Ⓓ needs. Measured 155 KB/page, so "
                 "≈3.8 GB for all 317,907 courses. No API is involved: the page "
                 "is server-rendered and issues zero data requests.",
                 sk_courses.run_course_listing, depends_on=["detail"]),
        vb.Phase("course_detail", "Ⓓ Course detail (optional)",
                 "One request per COURSE — the expensive pass. The page carries "
                 "89 top-level keys; 75 are stored. Adds what the listing has "
                 "not got: specialization and its id, stream/substream ids, "
                 "course level (UG/PG), credential, education type, delivery "
                 "method, medium; the full fee BREAKDOWN — tuition, one-time, "
                 "hostel, deposit, other, what the total includes, the fee "
                 "year, the brochure and the prose that qualifies each figure; "
                 "eligibility with CATEGORY-WISE class-XII cutoffs and per-exam "
                 "cutoffs; the ordered admission steps; seats by "
                 "category/exam/domicile; placements with the GRAIN of the "
                 "salary figure; recruiters; the affiliating university; "
                 "highlights; dated events. Measured 144 KB each: ≈45.8 GB for "
                 "all 317,907. Use max_courses or a bandwidth budget and let it "
                 "drain over several runs.",
                 sk_courses.run_course_detail, depends_on=["courses"]),
    ],
)

vb.register(SHIKSHA)
