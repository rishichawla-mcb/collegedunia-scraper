"""
Shiksha — data layer. A SEPARATE DATABASE FILE, not just separate tables.

Owner instruction, 2026-09-24: *"we will keep both db separate"*. Course Finder
and Study Abroad share `data.db` with the domestic scrape (own prefixes, one
disk); Shiksha does not. It lives in its own SQLite file so a Shiksha crawl can
never grow, lock, corrupt or fill the Collegedunia database, and so either one
can be moved, backed up or dropped on its own.

Reuses ONLY generic infrastructure:
  * `db.connect`         — the WAL connection helper
  * `db.redact_secrets`  — credential stripping when a job config is persisted
  * `freshness`          — change tracking; it is db- and table-agnostic

It reads and writes nothing outside its own file.

Tables
------
sk_colleges          one row per DISTINCT college id (~57,695 expected).
sk_college_aliases   every slug seen for a college. The sitemaps publish 84,193
                     "college home" URLs for 57,695 ids — ~26k alias slugs. We
                     crawl by id, but the aliases are kept, never discarded:
                     they are how an old URL still resolves to the right row.
sk_universities      /university/<slug>-<id>.
sk_courses           course ids/slugs learned from offering URLs (free).
sk_offerings         (college_id, course_id) edges — college × course pages.
sk_sitemap_progress  per-sitemap resume state for discovery.
sk_college_progress  the self-draining detail queue.
sk_jobs / sk_logs / sk_settings   own bookkeeping.

Discovery is near-free: the whole inventory, ids included, is published in ~48
gzipped sitemaps. Nothing here fetches a college page.
"""
from __future__ import annotations

BUILD = "2026-09-24a"

import json
import os
import time
from typing import Any, Dict, List, Optional

import db as _core          # generic WAL connection + redact_secrets only
import freshness as _fr


def _redact(config):
    try:
        return _core.redact_secrets(config)
    except Exception:  # noqa: BLE001
        return config


# Its OWN file. Defaults to a sibling of the Collegedunia database so it lands on
# the same Render disk without any extra configuration, but it is a different
# file and can be pointed anywhere (a second disk, a different mount) with
# CD_SK_DB_PATH alone.
SK_DB_PATH = os.environ.get("CD_SK_DB_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(_core.DB_PATH)), "shiksha.db")


def connect(db_path: str = SK_DB_PATH):
    return _core.connect(db_path)


SCHEMA = """
-- One row per DISTINCT college id. The id comes out of the URL
-- (/college/<slug>-<id>), so it is known before a single page is fetched.
CREATE TABLE IF NOT EXISTS sk_colleges (
    college_id      INTEGER PRIMARY KEY,
    slug            TEXT,          -- canonical slug (the shortest one seen)
    url             TEXT,          -- canonical absolute home URL
    name            TEXT,
    short_name      TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    college_type    TEXT,
    established     TEXT,
    website         TEXT,
    email           TEXT,
    phone           TEXT,
    address         TEXT,
    logo            TEXT,
    rating          REAL,
    reviews_count   INTEGER,
    courses_count   INTEGER,
    university_id   INTEGER,
    tabs            TEXT,          -- CSV of tab names the sitemap publishes
    alias_count     INTEGER DEFAULT 0,
    lastmod         TEXT,          -- sitemap <lastmod>, free change signal
    discovered_at   REAL,
    detail_json     TEXT,          -- __PRELOADED_STATE__ extract (phase B)
    detail_scraped_at REAL,
    raw_json        TEXT, scraped_at REAL, source_job_id INTEGER
);
CREATE INDEX IF NOT EXISTS sk_idx_col_city  ON sk_colleges(city);
CREATE INDEX IF NOT EXISTS sk_idx_col_state ON sk_colleges(state);
CREATE INDEX IF NOT EXISTS sk_idx_col_univ  ON sk_colleges(university_id);

-- Every slug ever seen for an id. Kept, not deduplicated away.
CREATE TABLE IF NOT EXISTS sk_college_aliases (
    slug        TEXT PRIMARY KEY,
    college_id  INTEGER,
    url         TEXT,
    first_seen_at REAL,
    source_job_id INTEGER
);
CREATE INDEX IF NOT EXISTS sk_idx_alias_col ON sk_college_aliases(college_id);

CREATE TABLE IF NOT EXISTS sk_universities (
    university_id   INTEGER PRIMARY KEY,
    slug            TEXT,
    url             TEXT,
    name            TEXT,
    city            TEXT,
    state           TEXT,
    lastmod         TEXT,
    discovered_at   REAL,
    detail_json     TEXT,
    detail_scraped_at REAL,
    raw_json        TEXT, scraped_at REAL, source_job_id INTEGER
);

-- Course ids/slugs, learned for free from offering URLs.
CREATE TABLE IF NOT EXISTS sk_courses (
    course_id     INTEGER PRIMARY KEY,
    slug          TEXT,
    name          TEXT,
    level         TEXT,
    duration      TEXT,
    colleges_count INTEGER DEFAULT 0,
    discovered_at REAL,
    raw_json      TEXT, scraped_at REAL, source_job_id INTEGER
);

-- college x course. /college/<slug>-<id>/course-<cslug>-<courseId> carries BOTH
-- ids, so every edge is free from the sitemap — no request per offering.
CREATE TABLE IF NOT EXISTS sk_offerings (
    college_id    INTEGER,
    course_id     INTEGER,
    url           TEXT,
    course_slug   TEXT,
    course_name   TEXT,
    fees_amount   INTEGER,
    fees_text     TEXT,
    duration      TEXT,
    level         TEXT,
    exams         TEXT,
    lastmod       TEXT,
    discovered_at REAL,
    raw_json      TEXT, scraped_at REAL, source_job_id INTEGER,
    PRIMARY KEY (college_id, course_id)
);
CREATE INDEX IF NOT EXISTS sk_idx_off_course ON sk_offerings(course_id);

-- Discovery resume state, one row per sitemap file.
CREATE TABLE IF NOT EXISTS sk_sitemap_progress (
    sitemap_url TEXT PRIMARY KEY,
    kind        TEXT,              -- 'college' | 'university' | 'listing'
    status      TEXT,              -- 'done' | 'error'
    urls        INTEGER DEFAULT 0,
    colleges    INTEGER DEFAULT 0,
    universities INTEGER DEFAULT 0,
    offerings   INTEGER DEFAULT 0,
    bytes       INTEGER DEFAULT 0,
    lastmod     TEXT,
    message     TEXT,
    updated_at  REAL
);
CREATE INDEX IF NOT EXISTS sk_idx_smap_status ON sk_sitemap_progress(status);

-- The self-draining detail queue: a college leaves colleges_pending() the
-- moment it is marked done.
CREATE TABLE IF NOT EXISTS sk_college_progress (
    college_id  INTEGER PRIMARY KEY,
    status      TEXT,              -- 'done' | 'partial' | 'gone' | 'error'
    found       INTEGER DEFAULT 0,
    message     TEXT,
    updated_at  REAL
);
CREATE INDEX IF NOT EXISTS sk_idx_cprog_status ON sk_college_progress(status);

-- Phase Ⓒ's own queue. Separate from sk_college_progress on purpose: a college
-- can be 'done' for detail and still owe its course listing, and merging the two
-- would either re-crawl finished detail or hide the course backlog.
CREATE TABLE IF NOT EXISTS sk_course_progress (
    college_id  INTEGER PRIMARY KEY,
    status      TEXT,              -- 'done' | 'gone' | 'error'
    pages       INTEGER DEFAULT 0, -- listing pages actually fetched
    found       INTEGER DEFAULT 0, -- course rows written
    expected    INTEGER,           -- the site's own totalCourses, for auditing
    message     TEXT,
    updated_at  REAL
);
CREATE INDEX IF NOT EXISTS sk_idx_krprog_status ON sk_course_progress(status);

CREATE TABLE IF NOT EXISTS sk_jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    vertical      TEXT,
    phase         TEXT,
    status        TEXT,
    config_json   TEXT,
    total_units   INTEGER DEFAULT 0,
    done_units    INTEGER DEFAULT 0,
    items_written INTEGER DEFAULT 0,
    req_count     INTEGER DEFAULT 0,
    bytes_count   INTEGER DEFAULT 0,
    message       TEXT,
    stop_requested INTEGER DEFAULT 0,
    pid           INTEGER,
    started_at    REAL, updated_at REAL, finished_at REAL
);
CREATE TABLE IF NOT EXISTS sk_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, ts REAL, message TEXT
);
CREATE INDEX IF NOT EXISTS sk_idx_logs_job ON sk_logs(job_id, id);

CREATE TABLE IF NOT EXISTS sk_settings (key TEXT PRIMARY KEY, value TEXT);

-- Shiksha's SHARED course catalogue. `baseCourseTuples[].id` ("B.Des" = 9) is a
-- site-wide id that never appears in a URL, which is why sitemap discovery could
-- not see it and `sk_courses` ended up a page index instead of a catalogue.
-- A few hundred rows, and the join key the Collegedunia comparison needs.
CREATE TABLE IF NOT EXISTS sk_base_courses (
    base_course_id INTEGER PRIMARY KEY,
    name           TEXT,
    level          TEXT,
    colleges_count INTEGER DEFAULT 0,
    scraped_at     REAL, source_job_id INTEGER
);

-- The fees table: one row per college x base course.
-- NOTE the grain. A tuple is grouped by BASE course, so its fee range can span
-- several actual courses; `course_count` says how many, so a consumer can tell a
-- single-course range from an eight-course one rather than assuming precision
-- that is not there.
CREATE TABLE IF NOT EXISTS sk_college_base_courses (
    college_id      INTEGER,
    base_course_id  INTEGER,
    name            TEXT,
    course_page_id  INTEGER,       -- the per-college id used in /course-…-<id>
    level           TEXT,
    min_fees        INTEGER,
    max_fees        INTEGER,
    duration        TEXT,
    total_seats     INTEGER,
    course_count    INTEGER,
    rating          REAL,
    rating_count    INTEGER,
    money_rating    REAL,
    placement_rating REAL,
    min_salary      INTEGER,
    max_salary      INTEGER,
    eligibility_xii INTEGER,
    eligibility_grad INTEGER,
    scholarships_count INTEGER,
    exams           TEXT,
    ranking         TEXT,
    url             TEXT,
    scraped_at      REAL, source_job_id INTEGER,
    PRIMARY KEY (college_id, base_course_id)
);
CREATE INDEX IF NOT EXISTS sk_idx_cbc_base ON sk_college_base_courses(base_course_id);
CREATE INDEX IF NOT EXISTS sk_idx_cbc_fees ON sk_college_base_courses(min_fees);

-- Collegedunia college  <->  Shiksha college.
-- Lives in the SHIKSHA database on purpose: the Collegedunia file stays
-- untouched, so a bad matching run can be dropped by deleting this one table.
-- `tier` records WHICH evidence produced the pair, because "these two are the
-- same college" is a claim of very different strength depending on whether it
-- came from a shared website domain or from two names looking alike.
CREATE TABLE IF NOT EXISTS sk_matches (
    cd_college_id INTEGER,
    sk_college_id INTEGER,
    score         REAL,
    tier          TEXT,     -- website|phone|email|shortform|name
    evidence      TEXT,     -- JSON: the signals that agreed, and their values
    verdict       TEXT,     -- yes | no | pending
    decided_by    TEXT,     -- auto | judge | human
    note          TEXT,
    decided_at    REAL,
    PRIMARY KEY (cd_college_id, sk_college_id)
);
CREATE INDEX IF NOT EXISTS sk_idx_match_sk  ON sk_matches(sk_college_id);
CREATE INDEX IF NOT EXISTS sk_idx_match_v   ON sk_matches(verdict);
CREATE INDEX IF NOT EXISTS sk_idx_match_t   ON sk_matches(tier);
"""

# Columns phase Ⓑ adds to sk_colleges. `CREATE TABLE IF NOT EXISTS` does nothing
# to a table that already exists, so a database built by discovery needs these
# added explicitly — without this the detail writer would fail on every row of a
# live database while passing every test against a fresh one.
DETAIL_COLUMNS = [
    ("ownership", "TEXT"), ("tier", "TEXT"), ("city_id", "INTEGER"),
    ("state_id", "INTEGER"), ("locality", "TEXT"), ("country_id", "INTEGER"),
    ("questions_count", "INTEGER"), ("photo_count", "INTEGER"),
    ("video_count", "INTEGER"), ("base_course_count", "INTEGER"),
    ("flagship_course_id", "INTEGER"), ("parent_university", "TEXT"),
    ("affiliations", "TEXT"), ("facilities", "TEXT"), ("recruiters", "TEXT"),
    ("highlights", "TEXT"), ("streams", "TEXT"), ("accepted_exams", "TEXT"),
    ("rankings", "TEXT"), ("admission_text", "TEXT"),
    ("admission_updated", "TEXT"), ("description", "TEXT"),
    ("meta_title", "TEXT"), ("meta_description", "TEXT"),
    ("canonical_url", "TEXT"), ("tab_urls", "TEXT"),
    ("latitude", "REAL"), ("longitude", "REAL"),
    ("reviews_verified", "INTEGER"),
]

# Columns phase Ⓒ adds to sk_offerings, for the same reason DETAIL_COLUMNS exist:
# the table was created by discovery and CREATE TABLE IF NOT EXISTS will not
# touch it. The first block comes from the listing page, the second only from the
# per-course page — kept apart here so it stays obvious which pass fills what,
# and so a database with the listing pass done but not the deep pass reads as
# "specialization not collected yet" rather than "this course has none".
OFFERING_COLUMNS = [
    # from /courses (cheap: ~1 page per 12 courses)
    ("base_course_id", "INTEGER"), ("base_course_name", "TEXT"),
    ("total_seats", "INTEGER"), ("median_salary", "INTEGER"),
    ("rating", "REAL"), ("rating_count", "INTEGER"), ("money_rating", "REAL"),
    ("scholarships_count", "INTEGER"), ("eligibility_x", "INTEGER"),
    ("eligibility_xii", "INTEGER"), ("eligibility_grad", "INTEGER"),
    ("eligibility_pg", "INTEGER"), ("work_experience", "TEXT"),
    ("difficulty_level", "TEXT"), ("skills", "TEXT"), ("credential", "TEXT"),
    ("is_online", "INTEGER"), ("curriculum_pdf", "TEXT"),
    ("intake_dates", "TEXT"), ("commencement_dates", "TEXT"),
    ("admission_status", "TEXT"), ("shiksha_rank", "INTEGER"),
    ("exams_count", "INTEGER"), ("institute_grade", "TEXT"),
    ("ranking", "TEXT"), ("listed_at", "REAL"),
    # from /course-<slug>-<id> (expensive: 1 page per course, 144 KB measured).
    # The page carries 89 top-level keys; these are the ones that are DATA about
    # the course rather than page furniture (widgets, SEO blocks, breadcrumbs,
    # author details, A/B variants, "also viewed" carousels).
    #   taxonomy
    ("specialization", "TEXT"), ("specialization_id", "INTEGER"),
    ("substream", "TEXT"), ("substream_id", "INTEGER"),
    ("stream_id", "INTEGER"), ("course_level", "TEXT"),
    ("education_type", "TEXT"), ("delivery_method", "TEXT"),
    ("medium", "TEXT"), ("course_type", "TEXT"), ("institute_type", "TEXT"),
    ("course_variant", "INTEGER"), ("is_course_paid", "INTEGER"),
    ("nzqf", "TEXT"),
    #   fees — scalars are the `general` category, the _json twins keep the
    #   whole category-keyed map so a college that ever publishes SC/ST/OBC
    #   figures does not lose them to a scalar that only looked at `general`.
    ("fees_total", "INTEGER"), ("fees_onetime", "INTEGER"),
    ("fees_hostel", "INTEGER"), ("fees_deposit", "INTEGER"),
    ("fees_other", "INTEGER"),
    ("fees_total_json", "TEXT"), ("fees_onetime_json", "TEXT"),
    ("fees_hostel_json", "TEXT"), ("fees_deposit_json", "TEXT"),
    ("fees_period_json", "TEXT"), ("fees_period_type", "TEXT"),
    ("fees_includes", "TEXT"), ("fees_categories", "TEXT"),
    ("fees_location_json", "TEXT"), ("fees_year", "INTEGER"),
    ("fees_currency", "TEXT"), ("fees_note", "TEXT"),
    ("fees_hostel_note", "TEXT"), ("fees_onetime_note", "TEXT"),
    ("fees_deposit_note", "TEXT"), ("fees_brochure_url", "TEXT"),
    #   eligibility
    ("elig_year", "INTEGER"), ("elig_x_json", "TEXT"), ("elig_xii_json", "TEXT"),
    ("elig_grad_json", "TEXT"), ("elig_pg_json", "TEXT"),
    ("elig_xii_general", "INTEGER"), ("elig_xii_scores", "TEXT"),
    ("elig_xii_score_type", "TEXT"), ("elig_exams_json", "TEXT"),
    ("elig_min_workex", "INTEGER"), ("elig_max_workex", "INTEGER"),
    ("elig_min_age", "INTEGER"), ("elig_max_age", "INTEGER"),
    ("elig_backlogs", "INTEGER"), ("elig_note", "TEXT"),
    ("elig_intl_note", "TEXT"),
    #   structure, admission, seats
    ("course_period", "TEXT"), ("period_courses_json", "TEXT"),
    ("admission_steps", "TEXT"), ("seats_total", "INTEGER"),
    ("seats_category_json", "TEXT"), ("seats_exam_json", "TEXT"),
    ("seats_domicile_json", "TEXT"),
    #   placements — `placement_grain` records WHAT the salary describes
    ("placement_grain", "TEXT"), ("placement_batch_year", "INTEGER"),
    ("placement_pct", "REAL"), ("salary_avg", "INTEGER"),
    ("salary_median", "INTEGER"), ("salary_max", "INTEGER"),
    ("salary_min", "INTEGER"), ("salary_currency", "TEXT"),
    ("placement_report_url", "TEXT"), ("internships_available", "INTEGER"),
    ("recruiters", "TEXT"),
    #   affiliation, highlights, dates
    ("affiliation_university_id", "INTEGER"), ("affiliation_name", "TEXT"),
    ("affiliation_url", "TEXT"), ("affiliation_scope", "TEXT"),
    ("highlights", "TEXT"), ("important_dates_json", "TEXT"),
    ("deep_scraped_at", "REAL"),
]

# The Ⓓ-only columns, named once so OFFERING_LISTING_COLS and OFFERING_DEEP_COLS
# cannot drift apart. `curriculum_pdf` is deliberately NOT here: phase Ⓒ already
# fills it from the listing tuple, and phase Ⓓ refreshes it from
# courseStructure, so it belongs to both.
DEEP_ONLY = {
    "specialization", "specialization_id", "substream", "substream_id",
    "stream_id", "course_level", "education_type", "delivery_method", "medium",
    "course_type", "institute_type", "course_variant", "is_course_paid", "nzqf",
    "fees_total", "fees_onetime", "fees_hostel", "fees_deposit", "fees_other",
    "fees_total_json", "fees_onetime_json", "fees_hostel_json",
    "fees_deposit_json", "fees_period_json", "fees_period_type",
    "fees_includes", "fees_categories", "fees_location_json", "fees_year",
    "fees_currency", "fees_note", "fees_hostel_note", "fees_onetime_note",
    "fees_deposit_note", "fees_brochure_url",
    "elig_year", "elig_x_json", "elig_xii_json", "elig_grad_json",
    "elig_pg_json", "elig_xii_general", "elig_xii_scores",
    "elig_xii_score_type", "elig_exams_json", "elig_min_workex",
    "elig_max_workex", "elig_min_age", "elig_max_age", "elig_backlogs",
    "elig_note", "elig_intl_note",
    "course_period", "period_courses_json", "admission_steps", "seats_total",
    "seats_category_json", "seats_exam_json", "seats_domicile_json",
    "placement_grain", "placement_batch_year", "placement_pct", "salary_avg",
    "salary_median", "salary_max", "salary_min", "salary_currency",
    "placement_report_url", "internships_available", "recruiters",
    "affiliation_university_id", "affiliation_name", "affiliation_url",
    "affiliation_scope", "highlights", "important_dates_json",
    "deep_scraped_at",
}


def init_db(db_path: str = SK_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        # The change log belongs to the FILE, not to any one table. In data.db it
        # already exists because the domestic schema created it; a brand-new
        # Shiksha file has nothing, and freshness only builds it lazily inside
        # ensure_schema() — which it skips for any table already in its
        # process-global _SCHEMA_READY cache. Creating it here removes the order
        # dependency entirely. (Found 2026-09-24: every sitemap failed with
        # "no such table: data_changes" while the colleges themselves parsed
        # fine.)
        conn.executescript(_fr.CHANGE_LOG_DDL)
        have = {r[1] for r in conn.execute("PRAGMA table_info(sk_colleges)")}
        for col, typ in DETAIL_COLUMNS:
            if col not in have:
                conn.execute(f"ALTER TABLE sk_colleges ADD COLUMN {col} {typ}")
        have = {r[1] for r in conn.execute("PRAGMA table_info(sk_offerings)")}
        for col, typ in OFFERING_COLUMNS:
            if col not in have:
                conn.execute(f"ALTER TABLE sk_offerings ADD COLUMN {col} {typ}")


# ---------------------------------------------------------------------------
# Writes — non-destructive upserts, identical contract to cf_db/sa_db: a blank
# incoming value never overwrites something already stored. Discovery writes a
# thin row (id, slug, url, lastmod); detail later fills name/city/rating. Without
# preserve_nonempty a re-run of discovery would blank everything detail found.
# ---------------------------------------------------------------------------
COLLEGE_COLS = ["college_id", "slug", "url", "name", "short_name", "city", "state",
                "country", "college_type", "established", "website", "email",
                "phone", "address", "logo", "rating", "reviews_count",
                "courses_count", "university_id", "tabs", "alias_count", "lastmod",
                "discovered_at", "detail_json", "detail_scraped_at",
                "raw_json", "scraped_at", "source_job_id"]

ALIAS_COLS = ["slug", "college_id", "url", "first_seen_at", "source_job_id"]

UNIVERSITY_COLS = ["university_id", "slug", "url", "name", "city", "state",
                   "lastmod", "discovered_at", "detail_json", "detail_scraped_at",
                   "raw_json", "scraped_at", "source_job_id"]

COURSE_COLS = ["course_id", "slug", "name", "level", "duration", "colleges_count",
               "discovered_at", "raw_json", "scraped_at", "source_job_id"]

OFFERING_COLS = ["college_id", "course_id", "url", "course_slug", "course_name",
                 "fees_amount", "fees_text", "duration", "level", "exams",
                 "lastmod", "discovered_at", "raw_json", "scraped_at",
                 "source_job_id"]


def _job_of(rows):
    """The job that produced this batch, read off the rows themselves."""
    for r in rows:
        j = r.get("source_job_id")
        if j is not None:
            return j
    return None


def _upsert(conn, table: str, cols: List[str], key_cols: List[str],
            rows: List[Dict[str, Any]], preserve_nonempty: bool = True) -> int:
    if not rows:
        return 0
    ph = ",".join("?" for _ in cols)
    if preserve_nonempty:
        setc = ",".join(
            f"{c}=CASE WHEN excluded.{c} IS NULL OR CAST(excluded.{c} AS TEXT)='' "
            f"THEN {table}.{c} ELSE excluded.{c} END"
            for c in cols if c not in key_cols)
    else:
        setc = ",".join(f"{c}=excluded.{c}" for c in cols if c not in key_cols)
    with _fr.tracking(conn, table, key_cols, rows, _job_of(rows)):
        conn.executemany(
            f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) "
            f"ON CONFLICT({','.join(key_cols)}) DO UPDATE SET {setc}",
            [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


def upsert_colleges(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows if r.get("college_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_colleges", COLLEGE_COLS, ["college_id"], rows)


def upsert_aliases(rows, db_path: str = SK_DB_PATH) -> int:
    """Alias slugs. Not change-tracked: an alias is an immutable fact about a
    URL, and 26k of them would otherwise flood the change log on first run."""
    rows = [r for r in rows if r.get("slug") and r.get("college_id") is not None]
    if not rows:
        return 0
    ph = ",".join("?" for _ in ALIAS_COLS)
    with connect(db_path) as conn:
        conn.executemany(
            f"INSERT INTO sk_college_aliases ({','.join(ALIAS_COLS)}) VALUES ({ph}) "
            f"ON CONFLICT(slug) DO NOTHING",
            [tuple(r.get(c) for c in ALIAS_COLS) for r in rows])
    return len(rows)


def upsert_universities(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows if r.get("university_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_universities", UNIVERSITY_COLS,
                       ["university_id"], rows)


def upsert_courses(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows if r.get("course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_courses", COURSE_COLS, ["course_id"], rows)


def upsert_offerings(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows
            if r.get("college_id") is not None and r.get("course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_offerings", OFFERING_COLS,
                       ["college_id", "course_id"], rows)


# --------------------------------------------------------------------------- Ⓒ
# The listing pass writes the discovery columns AND the new ones; the deep pass
# writes only what the listing cannot give. Two column lists rather than one so
# the deep pass cannot silently blank a listing value it does not carry.
OFFERING_LISTING_COLS = OFFERING_COLS + [c for c, _ in OFFERING_COLUMNS
                                         if c not in DEEP_ONLY]

OFFERING_DEEP_COLS = (["college_id", "course_id", "course_name",
                       "base_course_name", "credential", "duration",
                       "curriculum_pdf", "source_job_id"]
                      + [c for c, _ in OFFERING_COLUMNS if c in DEEP_ONLY])


def upsert_offering_listing(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows
            if r.get("college_id") is not None and r.get("course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_offerings", OFFERING_LISTING_COLS,
                       ["college_id", "course_id"], rows)


def upsert_offering_deep(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows
            if r.get("college_id") is not None and r.get("course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_offerings", OFFERING_DEEP_COLS,
                       ["college_id", "course_id"], rows)


def set_course_progress(college_id: int, status: str, pages: int = 0,
                        found: int = 0, expected: Optional[int] = None,
                        message: str = "", db_path: str = SK_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sk_course_progress"
            "(college_id,status,pages,found,expected,message,updated_at) "
            "VALUES(?,?,?,?,?,?,?) ON CONFLICT(college_id) DO UPDATE SET "
            "status=excluded.status,pages=excluded.pages,found=excluded.found,"
            "expected=excluded.expected,message=excluded.message,"
            "updated_at=excluded.updated_at",
            (int(college_id), status, int(pages), int(found),
             None if expected is None else int(expected), message[:500],
             time.time()))


def colleges_pending_courses(limit: int = 0, order: str = "value",
                             db_path: str = SK_DB_PATH) -> List[Dict[str, Any]]:
    """Self-draining phase Ⓒ queue.

    Only colleges phase Ⓑ has already resolved: a college whose home page is
    'gone' has no course listing either, and asking for one would spend a
    request to be told so again. Default order is 'value' — most known offerings
    first — so a run stopped by a budget has collected the courses that matter
    rather than the numerically lowest ids."""
    order_sql = ("(SELECT COUNT(*) FROM sk_offerings o WHERE o.college_id=c.college_id) DESC"
                 if order == "value" else "c.college_id ASC")
    sql = ("SELECT c.college_id, c.slug, c.url FROM sk_colleges c "
           "JOIN sk_college_progress d ON d.college_id=c.college_id "
           "                          AND d.status='done' "
           "LEFT JOIN sk_course_progress p ON p.college_id=c.college_id "
           "WHERE p.college_id IS NULL OR p.status NOT IN ('done','gone') "
           f"ORDER BY {order_sql}")
    if limit:
        sql += f" LIMIT {int(limit)}"
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql)]


def offerings_pending_deep(limit: int = 0,
                           db_path: str = SK_DB_PATH) -> List[Dict[str, Any]]:
    """Courses whose per-course page has not been fetched. Requires the listing
    pass first, because the per-course URL comes from it."""
    sql = ("SELECT college_id, course_id, url FROM sk_offerings "
           "WHERE deep_scraped_at IS NULL AND url IS NOT NULL AND url<>'' "
           "ORDER BY college_id, course_id")
    if limit:
        sql += f" LIMIT {int(limit)}"
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql)]


# --------------------------------------------------------------------------- Ⓑ
COLLEGE_DETAIL_COLS = [
    "college_id", "name", "short_name", "college_type", "ownership", "tier",
    "city", "state", "city_id", "state_id", "locality", "country_id", "address",
    "website", "phone", "email", "logo", "rating", "reviews_count",
    "questions_count", "photo_count", "video_count", "base_course_count",
    "flagship_course_id", "parent_university", "affiliations", "facilities",
    "recruiters", "highlights", "streams", "accepted_exams", "rankings",
    "admission_text", "admission_updated", "description", "meta_title",
    "meta_description", "canonical_url", "tab_urls", "latitude", "longitude",
    "reviews_verified", "detail_scraped_at", "scraped_at", "source_job_id",
]

BASE_COURSE_COLS = ["base_course_id", "name", "level", "scraped_at",
                    "source_job_id"]

COLLEGE_BASE_COURSE_COLS = [
    "college_id", "base_course_id", "name", "course_page_id", "level",
    "min_fees", "max_fees", "duration", "total_seats", "course_count", "rating",
    "rating_count", "money_rating", "placement_rating", "min_salary",
    "max_salary", "eligibility_xii", "eligibility_grad", "scholarships_count",
    "exams", "ranking", "url", "scraped_at", "source_job_id",
]


def upsert_college_detail(rows, db_path: str = SK_DB_PATH) -> int:
    """Phase Ⓑ attributes onto the rows discovery created.

    Non-destructive as everywhere else, which matters more here than usual:
    discovery owns `slug`, `url`, `tabs`, `alias_count` and `lastmod`, and detail
    must not blank any of them."""
    rows = [r for r in rows if r.get("college_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_colleges", COLLEGE_DETAIL_COLS,
                       ["college_id"], rows)


def upsert_base_courses(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows if r.get("base_course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_base_courses", BASE_COURSE_COLS,
                       ["base_course_id"], rows)


def upsert_college_base_courses(rows, db_path: str = SK_DB_PATH) -> int:
    rows = [r for r in rows if r.get("college_id") is not None
            and r.get("base_course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sk_college_base_courses", COLLEGE_BASE_COURSE_COLS,
                       ["college_id", "base_course_id"], rows)


def recount_base_course_colleges(db_path: str = SK_DB_PATH) -> int:
    """Fill sk_base_courses.colleges_count from the discovered edges — the
    catalogue's own 'how many colleges offer this', computed, never guessed."""
    with connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE sk_base_courses SET colleges_count = COALESCE(("
            "  SELECT COUNT(*) FROM sk_college_base_courses c "
            "  WHERE c.base_course_id = sk_base_courses.base_course_id), 0)")
        return cur.rowcount or 0


def recount_course_colleges(db_path: str = SK_DB_PATH) -> int:
    """Fill sk_courses.colleges_count from the discovered edges. This is the
    Shiksha equivalent of cf_courses.colleges_count: it costs nothing and it is
    what lets phase B be forecast and prioritised BEFORE spending a request."""
    with connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE sk_courses SET colleges_count = COALESCE(("
            "  SELECT COUNT(*) FROM sk_offerings o "
            "  WHERE o.course_id = sk_courses.course_id), 0)")
        return cur.rowcount or 0


# ---------------------------------------------------------------------------
# Discovery progress / resume
# ---------------------------------------------------------------------------
def set_sitemap(sitemap_url: str, kind: str, status: str, urls: int = 0,
                colleges: int = 0, universities: int = 0, offerings: int = 0,
                bytes_: int = 0, lastmod: Optional[str] = None,
                message: str = "", db_path: str = SK_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sk_sitemap_progress(sitemap_url,kind,status,urls,colleges,"
            "universities,offerings,bytes,lastmod,message,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(sitemap_url) DO UPDATE SET "
            "kind=excluded.kind,status=excluded.status,urls=excluded.urls,"
            "colleges=excluded.colleges,universities=excluded.universities,"
            "offerings=excluded.offerings,bytes=excluded.bytes,"
            "lastmod=excluded.lastmod,message=excluded.message,"
            "updated_at=excluded.updated_at",
            (sitemap_url, kind, status, int(urls), int(colleges),
             int(universities), int(offerings), int(bytes_), lastmod, message,
             time.time()))


def done_sitemaps(db_path: str = SK_DB_PATH) -> set:
    with connect(db_path) as conn:
        return {r[0] for r in conn.execute(
            "SELECT sitemap_url FROM sk_sitemap_progress WHERE status='done'")}


def set_college_progress(college_id: int, status: str, found: int = 0,
                         message: str = "", db_path: str = SK_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sk_college_progress(college_id,status,found,message,updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(college_id) DO UPDATE SET "
            "status=excluded.status,found=excluded.found,message=excluded.message,"
            "updated_at=excluded.updated_at",
            (int(college_id), status, int(found), message[:500], time.time()))


def colleges_pending(limit: int = 0, order: str = "id",
                     db_path: str = SK_DB_PATH) -> List[Dict[str, Any]]:
    """Self-draining detail queue: every college without a terminal progress row.

    'gone' counts as terminal — a page the site says does not exist is an answer,
    not a failure, and re-asking every run only burns requests."""
    order_sql = ("(SELECT COUNT(*) FROM sk_offerings o WHERE o.college_id=c.college_id) DESC"
                 if order == "value" else "c.college_id ASC")
    sql = ("SELECT c.college_id, c.slug, c.url FROM sk_colleges c "
           "LEFT JOIN sk_college_progress p ON p.college_id=c.college_id "
           "WHERE p.college_id IS NULL OR p.status NOT IN ('done','gone') "
           f"ORDER BY {order_sql}")
    if limit:
        sql += f" LIMIT {int(limit)}"
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql)]


def load_college_index(db_path: str = SK_DB_PATH) -> Dict[int, Dict[str, Any]]:
    """{college_id: {'slug','tabs','alias_count'}} for every known college.

    Discovery accumulates tabs and alias counts across ALL sitemaps: a college's
    home URL is in one file and its `/fees` tab may be in another. Those two
    columns therefore GROW, and an upsert that simply wrote the current run's
    view would shrink them on a resumed run — the second run sees only the
    sitemaps the first had not finished. Seeding the accumulator from here makes
    the written value a union rather than a replacement. One query, ~57k rows.
    """
    out: Dict[int, Dict[str, Any]] = {}
    with connect(db_path) as conn:
        try:
            rows = conn.execute(
                "SELECT college_id, slug, tabs, alias_count FROM sk_colleges")
        except Exception:  # noqa: BLE001  (table not created yet)
            return out
        for r in rows:
            out[int(r["college_id"])] = {
                "slug": r["slug"] or "",
                "tabs": [t for t in (r["tabs"] or "").split(",") if t],
                "alias_count": int(r["alias_count"] or 0),
            }
    return out


def counts(db_path: str = SK_DB_PATH) -> Dict[str, int]:
    with connect(db_path) as conn:
        def one(sql):
            try:
                return conn.execute(sql).fetchone()[0]
            except Exception:  # noqa: BLE001
                return 0
        return {
            "colleges": one("SELECT COUNT(*) FROM sk_colleges"),
            "aliases": one("SELECT COUNT(*) FROM sk_college_aliases"),
            "universities": one("SELECT COUNT(*) FROM sk_universities"),
            "courses": one("SELECT COUNT(*) FROM sk_courses"),
            "offerings": one("SELECT COUNT(*) FROM sk_offerings"),
            "colleges_with_detail": one("SELECT COUNT(*) FROM sk_colleges "
                                        "WHERE detail_scraped_at IS NOT NULL"),
            "base_courses": one("SELECT COUNT(*) FROM sk_base_courses"),
            "college_base_courses": one("SELECT COUNT(*) FROM sk_college_base_courses"),
            "colleges_done": one("SELECT COUNT(*) FROM sk_college_progress "
                                 "WHERE status IN ('done','gone')"),
            "course_listing_done": one("SELECT COUNT(*) FROM sk_course_progress "
                                       "WHERE status IN ('done','gone')"),
            "offerings_with_fees": one("SELECT COUNT(*) FROM sk_offerings "
                                       "WHERE fees_amount IS NOT NULL"),
            "offerings_listed": one("SELECT COUNT(*) FROM sk_offerings "
                                    "WHERE listed_at IS NOT NULL"),
            "offerings_deep": one("SELECT COUNT(*) FROM sk_offerings "
                                  "WHERE deep_scraped_at IS NOT NULL"),
            "offerings_with_spec": one("SELECT COUNT(*) FROM sk_offerings "
                                       "WHERE specialization IS NOT NULL "
                                       "AND specialization<>''"),
            "sitemaps_done": one("SELECT COUNT(*) FROM sk_sitemap_progress "
                                 "WHERE status='done'"),
        }


def detail_forecast(db_path: str = SK_DB_PATH,
                    kb_per_college: float = 170.0) -> Dict[str, Any]:
    """Cost phase B from what discovery already knows, before spending anything.

    170 KB/college is the measured wire cost of one Shiksha college home page
    (1,072 KB decompressed) — see the 2026-09-24 recon. It is the number that
    decides whether phase B is affordable at all, so it is reported, not hidden.
    """
    c = counts(db_path)
    left = max(0, c["colleges"] - c["colleges_done"])
    return {
        "colleges": c["colleges"],
        "colleges_done": c["colleges_done"],
        "colleges_left": left,
        "kb_per_college": kb_per_college,
        "est_gb_total": round(c["colleges"] * kb_per_college / 1024 / 1024, 2),
        "est_gb_left": round(left * kb_per_college / 1024 / 1024, 2),
    }


# ---------------------------------------------------------------------------
# Jobs / logs — own bookkeeping in its own file.
# ---------------------------------------------------------------------------
def create_job(phase: str, config: Dict[str, Any], db_path: str = SK_DB_PATH) -> int:
    now = time.time()
    with connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO sk_jobs(vertical,phase,status,config_json,started_at,updated_at) "
            "VALUES('shiksha',?,?,?,?,?)",
            (phase, "queued", json.dumps(_redact(config)), now, now))
        return cur.lastrowid


def update_job(job_id: int, db_path: str = SK_DB_PATH, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    sets = ",".join(f"{k}=?" for k in fields)
    with connect(db_path) as conn:
        conn.execute(f"UPDATE sk_jobs SET {sets} WHERE id=?",
                     (*fields.values(), job_id))


def get_job(job_id: int, db_path: str = SK_DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        r = conn.execute("SELECT * FROM sk_jobs WHERE id=?", (job_id,)).fetchone()
        return dict(r) if r else None


def list_jobs(limit: int = 50, db_path: str = SK_DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM sk_jobs ORDER BY id DESC LIMIT ?", (int(limit),))]


def stop_requested(job_id: int, db_path: str = SK_DB_PATH) -> bool:
    with connect(db_path) as conn:
        r = conn.execute("SELECT stop_requested FROM sk_jobs WHERE id=?",
                         (job_id,)).fetchone()
        return bool(r and r[0])


def request_stop(job_id: int, db_path: str = SK_DB_PATH) -> None:
    update_job(job_id, stop_requested=1, db_path=db_path)


def add_log(job_id: int, message: str, db_path: str = SK_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("INSERT INTO sk_logs(job_id,ts,message) VALUES(?,?,?)",
                     (job_id, time.time(), message))


def get_logs(job_id: int, limit: int = 400, db_path: str = SK_DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM sk_logs WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, int(limit)))]


def prune_logs(keep: int = 8000, db_path: str = SK_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("DELETE FROM sk_logs WHERE id NOT IN "
                     "(SELECT id FROM sk_logs ORDER BY id DESC LIMIT ?)", (int(keep),))


def get_setting(key: str, default: Any = None, db_path: str = SK_DB_PATH) -> Any:
    with connect(db_path) as conn:
        r = conn.execute("SELECT value FROM sk_settings WHERE key=?", (key,)).fetchone()
    if r is None:
        return default
    try:
        return json.loads(r["value"])
    except Exception:  # noqa: BLE001
        return r["value"]


def set_setting(key: str, value: Any, db_path: str = SK_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("INSERT INTO sk_settings(key,value) VALUES(?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, json.dumps(value)))
