"""
Study Abroad — data layer. COMPLETELY ISOLATED from the domestic scraper:
its own SQLite file (CD_SA_DB_PATH), its own `sa_`-prefixed tables, and its own
jobs/logs/progress bookkeeping. Nothing here reads or writes a domestic table.

Reuses ONLY generic infrastructure from the existing `db` module: the WAL
connection context manager (`db.connect`). Everything else is vertical-specific.

This module is the reference for how any future vertical's DB layer should look:
own file, own namespace, own bookkeeping, generic connection reuse.
"""
from __future__ import annotations

BUILD = "2026-07-23a"

import json
import os
import time
from typing import Any, Dict, List, Optional

import db as _core  # reuse db.connect (generic WAL connection) + the SHARED db file.


def _redact(config):
    """Never persist credentials into sa_jobs.config_json — the worker puts them
    back from env/settings at run time. Mirrors db.redact_secrets."""
    try:
        return _core.redact_secrets(config)
    except Exception:
        return config

# SHARED database, SEPARATE tables. Study Abroad lives in the SAME data.db as the
# domestic vertical but only ever touches its own `sa_`-prefixed tables — data
# stays logically isolated by namespace. Override with CD_SA_DB_PATH if you ever
# want to split it back into its own file (e.g. /data/sa_data.db).
SA_DB_PATH = os.environ.get("CD_SA_DB_PATH") or _core.DB_PATH


def connect(db_path: str = SA_DB_PATH):
    """Generic WAL connection, reused from the core db module but pointed at the
    ISOLATED Study Abroad database file."""
    return _core.connect(db_path)


# ---------------------------------------------------------------------------
# Schema — all sa_-prefixed. Includes its OWN jobs/logs/progress so the vertical
# is fully self-contained and can never collide with domestic bookkeeping.
# ---------------------------------------------------------------------------
SCHEMA = """
-- Dimension: countries (derived from program rows)
CREATE TABLE IF NOT EXISTS sa_countries (
    country_code TEXT PRIMARY KEY,          -- 'uk','usa' (from URL first segment)
    country_id   INTEGER,                   -- facet id (13=USA,12=UK,...)
    name         TEXT,
    raw_json     TEXT, scraped_at REAL, source_job_id INTEGER
);

-- Dimension: universities (derived from program rows)
CREATE TABLE IF NOT EXISTS sa_universities (
    university_id INTEGER PRIMARY KEY,      -- lead_params.college_id
    name          TEXT,
    country_code  TEXT,
    city          TEXT,
    university_url TEXT,                     -- canonical collegedunia URL (for enrichment)
    logo_url      TEXT,
    raw_json      TEXT, scraped_at REAL, source_job_id INTEGER
);
CREATE INDEX IF NOT EXISTS sa_idx_univ_country ON sa_universities(country_code);

-- Fact: programs (course @ university). Captures MAXIMUM fields.
CREATE TABLE IF NOT EXISTS sa_programs (
    program_id       INTEGER PRIMARY KEY,   -- course_id
    name             TEXT,                  -- head_one
    name_secondary   TEXT,                  -- head_two
    course_tags      TEXT,                  -- e.g. 'MBA'
    program_type     TEXT,                  -- 'on-campus'/online (attendance/mode)
    duration_text    TEXT,                  -- 'course_duration' raw
    duration_months  INTEGER,               -- parsed
    languages        TEXT,
    is_stem          INTEGER,
    is_partner       INTEGER,
    -- fees: BOTH native currency AND INR-normalized, with currency captured
    fee_native_raw   TEXT,                  -- 'GBP 88,800'
    fee_currency     TEXT,                  -- 'GBP'  (captured currency)
    fee_native_amount INTEGER,              -- 88800
    fee_inr_raw      TEXT,                  -- 'INR 1.2 Cr/Yr'
    fee_inr_amount   INTEGER,               -- 12000000
    fee_period       TEXT,                  -- 'per_year'
    application_end_date TEXT,
    -- identity / relationships
    university_id    INTEGER,
    university_name  TEXT,
    university_url   TEXT,
    country_code     TEXT,
    -- URLs for later enrichment
    program_url      TEXT,                  -- <-- program detail URL (enrichment target)
    logo_url         TEXT,
    -- ranking (embedded)
    ranking_rank     INTEGER,
    ranking_out_of   INTEGER,
    ranking_scope    TEXT,
    ranking_agency   TEXT,
    ranking_year     INTEGER,
    description      TEXT,
    raw_json         TEXT, scraped_at REAL, source_job_id INTEGER
);
CREATE INDEX IF NOT EXISTS sa_idx_prog_univ    ON sa_programs(university_id);
CREATE INDEX IF NOT EXISTS sa_idx_prog_country ON sa_programs(country_code);
CREATE INDEX IF NOT EXISTS sa_idx_prog_tags    ON sa_programs(course_tags);

-- Child: exam requirements (IELTS/TOEFL/GRE...)
CREATE TABLE IF NOT EXISTS sa_program_exams (
    program_id  INTEGER,
    exam_name   TEXT,
    short_form  TEXT,
    exam_score  TEXT,
    out_of      TEXT,
    median      TEXT,
    url         TEXT,
    PRIMARY KEY (program_id, short_form)
);

-- Study-abroad scholarships (collegedunia.com/scholarship).
-- Listing gives id/title/url + a label:value 'content' block; the detail page
-- adds highlights plus HTML blocks for eligibility / application / selection.
CREATE TABLE IF NOT EXISTS sa_scholarships (
    scholarship_id  INTEGER PRIMARY KEY,
    title           TEXT,
    url             TEXT,
    amount_text     TEXT,                   -- '₹1,436,100 ($15,000)'
    amount_inr      INTEGER,
    amount_native   INTEGER,
    amount_currency TEXT,
    scholarship_type TEXT,                  -- 'College-Specific, Merit-Based'
    level_of_study  TEXT,
    offered_by      TEXT,
    organization    TEXT,
    deadline        TEXT,
    num_scholarships INTEGER,
    renewability    TEXT,
    international_eligible TEXT,
    website_link    TEXT,
    countries       TEXT,
    description     TEXT,                   -- article.description (HTML)
    eligibility     TEXT,                   -- HTML block
    application     TEXT,                   -- HTML block
    selection       TEXT,                   -- HTML block
    detail_scraped_at REAL,
    raw_json        TEXT, scraped_at REAL, source_job_id INTEGER
);
CREATE INDEX IF NOT EXISTS sa_idx_schol_type ON sa_scholarships(scholarship_type);
CREATE INDEX IF NOT EXISTS sa_idx_schol_level ON sa_scholarships(level_of_study);

-- Per-partition progress for resumable, partitioned crawling.
CREATE TABLE IF NOT EXISTS sa_program_progress (
    partition_key TEXT PRIMARY KEY,         -- e.g. 'country=13|course_type=Bachelor'
    status        TEXT,                     -- 'partial'|'done'
    last_page     INTEGER DEFAULT 0,
    found         INTEGER DEFAULT 0,
    updated_at    REAL
);

-- Own jobs / logs (isolated bookkeeping)
CREATE TABLE IF NOT EXISTS sa_jobs (
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
    quality_score REAL,
    promote_status TEXT,
    staged_rows   INTEGER,
    started_at    REAL, updated_at REAL, finished_at REAL
);
CREATE TABLE IF NOT EXISTS sa_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, ts REAL, message TEXT
);
CREATE INDEX IF NOT EXISTS sa_idx_logs_job ON sa_logs(job_id, id);


-- ===================================================================
-- University DETAIL (phase ④). The programme listing only ever gave us a
-- university's name, country and URL; its own page carries ~40 more fields
-- plus a full ranking history, a cost-of-living breakdown, nearby places and
-- a per-course block. All additive, all sa_-prefixed.
-- ===================================================================
CREATE TABLE IF NOT EXISTS sa_university_rankings (
    university_id INTEGER,
    agency_id     INTEGER,
    agency        TEXT,
    year          INTEGER,
    stream        TEXT,          -- 'overall-I', 'Business-I', ...
    scope         TEXT,          -- Global / National
    rank          INTEGER,
    rank_out_of   INTEGER,
    country_rank  INTEGER,
    country_rank_out_of INTEGER,
    score         TEXT,
    score_out_of  TEXT,
    scraped_at    REAL, source_job_id INTEGER,
    PRIMARY KEY (university_id, agency_id, year, stream)
);
CREATE INDEX IF NOT EXISTS sa_idx_urank_univ ON sa_university_rankings(university_id);
CREATE INDEX IF NOT EXISTS sa_idx_urank_year ON sa_university_rankings(year);

CREATE TABLE IF NOT EXISTS sa_university_costs (
    university_id INTEGER,
    kind          TEXT,          -- 'on_campus' | 'off_campus' | 'overview'
    label         TEXT,          -- Food / Accommodation / Personal items / ...
    value         TEXT,
    scraped_at    REAL, source_job_id INTEGER,
    PRIMARY KEY (university_id, kind, label)
);

CREATE TABLE IF NOT EXISTS sa_university_nearby (
    university_id INTEGER,
    category      TEXT,          -- Airport / Train Station / Hospital / ...
    name          TEXT,
    distance_km   REAL,
    lat           TEXT,
    lng           TEXT,
    scraped_at    REAL, source_job_id INTEGER,
    PRIMARY KEY (university_id, category, name)
);
CREATE INDEX IF NOT EXISTS sa_idx_unear_univ ON sa_university_nearby(university_id);

-- One row per course offered by the university, straight off its page. Carries
-- the SEVEN-YEAR fee history and the multi-stage application deadlines that
-- appear nowhere in the programme listing.
CREATE TABLE IF NOT EXISTS sa_university_courses (
    university_id     INTEGER,
    course_id         INTEGER,
    head_one          TEXT,
    head_two          TEXT,
    head_short_form   TEXT,
    degree_type       TEXT,
    course_duration       TEXT,
    course_duration_value TEXT,
    total_fee_per_year    TEXT,
    default_fee_per_year  TEXT,
    fee_history_json  TEXT,       -- {"2021": "...", ... "2027": "..."}
    application_json  TEXT,       -- [{head_key, end_date}, ...] multi-stage
    application_cost  TEXT,
    short_entry_reqd  TEXT,       -- "IELTS: 7.5, TOEFL: 110, GMAT: Required, ..."
    available_campus  TEXT,
    program_url       TEXT,
    scraped_at        REAL, source_job_id INTEGER,
    PRIMARY KEY (university_id, course_id)
);
CREATE INDEX IF NOT EXISTS sa_idx_ucourse_univ ON sa_university_courses(university_id);
CREATE INDEX IF NOT EXISTS sa_idx_ucourse_course ON sa_university_courses(course_id);

-- Phase ④ progress. Self-draining queue: a university leaves it once done.
CREATE TABLE IF NOT EXISTS sa_university_progress (
    university_id INTEGER PRIMARY KEY,
    status        TEXT,          -- 'done' | 'partial' | 'error'
    found_courses INTEGER DEFAULT 0,
    found_rankings INTEGER DEFAULT 0,
    updated_at    REAL
);
CREATE INDEX IF NOT EXISTS sa_idx_uprog_status ON sa_university_progress(status);

CREATE TABLE IF NOT EXISTS sa_settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS sa_facets (
    filter_name TEXT, value_id TEXT, label TEXT, count INTEGER,
    updated_at REAL, PRIMARY KEY (filter_name, value_id)
);

-- Change-log snapshots of dataset size over time.
CREATE TABLE IF NOT EXISTS sa_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
    programs INTEGER, universities INTEGER, countries INTEGER,
    program_exams INTEGER, note TEXT
);

-- Governance: per-job staging buffer (stage -> validate -> promote), mirroring
-- the domestic engine but with its OWN table and only sa_ targets.
CREATE TABLE IF NOT EXISTS sa_staging (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     INTEGER,
    table_name TEXT,
    pk         TEXT,
    payload    TEXT,
    staged_at  REAL,
    UNIQUE(job_id, table_name, pk)
);
CREATE INDEX IF NOT EXISTS sa_idx_staging_job ON sa_staging(job_id, table_name);
"""


def init_db(db_path: str = SA_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        # Migrations: add columns to tables that pre-date them (CREATE TABLE IF NOT
        # EXISTS does NOT alter an existing table). Guarded + additive.
        jcols = {r[1] for r in conn.execute("PRAGMA table_info(sa_jobs)")}
        for col, typ in (("quality_score", "REAL"), ("promote_status", "TEXT"),
                         ("staged_rows", "INTEGER"), ("req_count", "INTEGER"),
                         ("bytes_count", "INTEGER"), ("total_units", "INTEGER"),
                         ("done_units", "INTEGER"), ("items_written", "INTEGER"),
                         ("pid", "INTEGER"), ("finished_at", "REAL")):
            if col not in jcols:
                conn.execute(f"ALTER TABLE sa_jobs ADD COLUMN {col} {typ}")
        # Phase ④ — university detail. `sa_universities` was derived from
        # programme rows and held 9 columns; its own page carries these.
        ucols = {r[1] for r in conn.execute("PRAGMA table_info(sa_universities)")}
        for col, typ in UNIVERSITY_DETAIL_COLS:
            if col not in ucols:
                conn.execute(f"ALTER TABLE sa_universities ADD COLUMN {col} {typ}")


# ---------------------------------------------------------------------------
# Upserts (idempotent — unique keys make duplicates impossible)
# ---------------------------------------------------------------------------
def _upsert(conn, table: str, cols: List[str], key_cols: List[str],
            rows: List[Dict[str, Any]], preserve_nonempty: bool = False) -> int:
    """preserve_nonempty=True makes the upsert non-destructive: an incoming NULL
    or '' never replaces a value already stored. Needed where more than one pass
    writes the same row — e.g. the scholarship listing pass carries no
    description/eligibility, and would otherwise blank what the detail pass
    wrote (and reset detail_scraped_at, re-queuing every row forever)."""
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
    sql = (f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph}) "
           f"ON CONFLICT({','.join(key_cols)}) DO UPDATE SET {setc}")
    conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


_PROG_COLS = ["program_id", "name", "name_secondary", "course_tags", "program_type",
              "duration_text", "duration_months", "languages", "is_stem", "is_partner",
              "fee_native_raw", "fee_currency", "fee_native_amount", "fee_inr_raw",
              "fee_inr_amount", "fee_period", "application_end_date", "university_id",
              "university_name", "university_url", "country_code", "program_url",
              "logo_url", "ranking_rank", "ranking_out_of", "ranking_scope",
              "ranking_agency", "ranking_year", "description", "raw_json",
              "scraped_at", "source_job_id"]
_UNIV_COLS = ["university_id", "name", "country_code", "city", "university_url",
              "logo_url", "raw_json", "scraped_at", "source_job_id"]
_CTRY_COLS = ["country_code", "country_id", "name", "raw_json", "scraped_at", "source_job_id"]
_EXAM_COLS = ["program_id", "exam_name", "short_form", "exam_score", "out_of", "median", "url"]


def upsert_programs(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows if r.get("program_id")]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_programs", _PROG_COLS, ["program_id"], rows)


def upsert_universities(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows if r.get("university_id")]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_universities", _UNIV_COLS, ["university_id"], rows)


def upsert_countries(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows if r.get("country_code")]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_countries", _CTRY_COLS, ["country_code"], rows)


def upsert_program_exams(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows if r.get("program_id") and r.get("short_form")]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_program_exams", _EXAM_COLS, ["program_id", "short_form"], rows)


_SCHOL_COLS = ["scholarship_id", "title", "url", "amount_text", "amount_inr",
               "amount_native", "amount_currency", "scholarship_type",
               "level_of_study", "offered_by", "organization", "deadline",
               "num_scholarships", "renewability", "international_eligible",
               "website_link", "countries", "description", "eligibility",
               "application", "selection", "detail_scraped_at", "raw_json",
               "scraped_at", "source_job_id"]

# Columns the DETAIL pass may fill. Never blanked — see update_scholarship_details.
_SCHOL_DETAIL_COLS = ["scholarship_type", "level_of_study", "offered_by",
                      "organization", "deadline", "num_scholarships",
                      "renewability", "international_eligible", "website_link",
                      "countries", "description", "eligibility", "application",
                      "selection", "amount_text", "amount_inr", "amount_native",
                      "amount_currency"]


def upsert_scholarships(rows, db_path: str = SA_DB_PATH) -> int:
    """Listing-pass writer. Non-destructive: re-running the listing must not
    blank the description/eligibility/application/selection the detail pass
    filled, nor reset detail_scraped_at (which would re-queue every row)."""
    rows = [r for r in rows if r.get("scholarship_id")]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_scholarships", _SCHOL_COLS, ["scholarship_id"],
                       rows, preserve_nonempty=True)


def update_scholarship_details(scholarship_id: int, fields: Dict[str, Any],
                               db_path: str = SA_DB_PATH) -> None:
    """Merge detail-page fields onto an existing scholarship row.

    NON-DESTRUCTIVE: an empty incoming value never replaces a value already
    stored (the listing pass fills some of these first). Only detail_scraped_at
    is unconditionally refreshed, so the row leaves the pending queue."""
    sets = ", ".join(
        f"{c}=CASE WHEN ? IS NULL OR ?='' THEN {c} ELSE ? END"
        for c in _SCHOL_DETAIL_COLS) + ", detail_scraped_at=?"
    vals: List[Any] = []
    for c in _SCHOL_DETAIL_COLS:
        v = fields.get(c)
        vals += [v, v, v]
    vals += [time.time(), scholarship_id]
    with connect(db_path) as conn:
        conn.execute(f"UPDATE sa_scholarships SET {sets} WHERE scholarship_id=?", vals)


def scholarships_pending_detail(db_path: str = SA_DB_PATH,
                                limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Self-draining work list: every scholarship whose detail page has not been
    fetched yet. A row drops out the moment it is filled, so a restart resumes
    automatically with no separate progress table."""
    sql = ("SELECT scholarship_id, url FROM sa_scholarships "
           "WHERE detail_scraped_at IS NULL AND COALESCE(url,'')<>'' "
           "ORDER BY scholarship_id")
    if limit:
        sql += f" LIMIT {int(limit)}"
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql).fetchall()]


def save_facets(rows, db_path: str = SA_DB_PATH) -> int:
    with connect(db_path) as conn:
        return _upsert(conn, "sa_facets",
                       ["filter_name", "value_id", "label", "count", "updated_at"],
                       ["filter_name", "value_id"], rows)


# ---------------------------------------------------------------------------
# Governance: staging -> validate -> promote (own tables; sa_ targets only)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Phase ④ — university detail
# ---------------------------------------------------------------------------
UNIVERSITY_DETAIL_COLS = [
    ("short_name", "TEXT"), ("state", "TEXT"), ("state_id", "INTEGER"),
    ("city_id", "INTEGER"), ("country_id", "INTEGER"), ("country_name", "TEXT"),
    ("established_year", "INTEGER"), ("institution_type", "TEXT"),
    ("school_type", "TEXT"), ("admin_rating", "REAL"),
    ("website", "TEXT"), ("address", "TEXT"), ("email", "TEXT"),
    ("phone", "TEXT"), ("toll_free", "TEXT"),
    ("latitude", "TEXT"), ("longitude", "TEXT"),
    ("total_students", "INTEGER"), ("total_faculty", "INTEGER"),
    ("faculty_full_time", "INTEGER"), ("faculty_part_time", "INTEGER"),
    ("graduate_assistants", "INTEGER"),
    ("avg_rating", "REAL"), ("total_reviews", "INTEGER"),
    ("rating_academic", "REAL"), ("rating_accommodation", "REAL"),
    ("rating_extracurricular", "REAL"), ("rating_faculty", "REAL"),
    ("rating_infrastructure", "REAL"), ("rating_placement", "REAL"),
    ("attendance_cost", "TEXT"), ("student_faculty_ratio", "TEXT"),
    ("cover_image", "TEXT"), ("description", "TEXT"),
    ("scholarship_count", "INTEGER"), ("course_count", "INTEGER"),
    ("detail_json", "TEXT"), ("detail_scraped_at", "REAL"),
]
UNIVERSITY_DETAIL_NAMES = [c for c, _ in UNIVERSITY_DETAIL_COLS]

_URANK_COLS = ["university_id", "agency_id", "agency", "year", "stream", "scope",
               "rank", "rank_out_of", "country_rank", "country_rank_out_of",
               "score", "score_out_of", "scraped_at", "source_job_id"]
_UCOST_COLS = ["university_id", "kind", "label", "value", "scraped_at", "source_job_id"]
_UNEAR_COLS = ["university_id", "category", "name", "distance_km", "lat", "lng",
               "scraped_at", "source_job_id"]
_UCOURSE_COLS = ["university_id", "course_id", "head_one", "head_two",
                 "head_short_form", "degree_type", "course_duration",
                 "course_duration_value", "total_fee_per_year",
                 "default_fee_per_year", "fee_history_json", "application_json",
                 "application_cost", "short_entry_reqd", "available_campus",
                 "program_url", "scraped_at", "source_job_id"]


def update_university_detail(university_id: int, fields: Dict[str, Any],
                             db_path: str = SA_DB_PATH) -> None:
    """Non-destructive: a blank incoming value never overwrites a stored one."""
    cols = [c for c in UNIVERSITY_DETAIL_NAMES
            if c not in ("detail_scraped_at",) and c in fields]
    if not cols:
        return
    sets = ", ".join(
        f"{c}=CASE WHEN ? IS NULL OR CAST(? AS TEXT)='' THEN {c} ELSE ? END"
        for c in cols)
    params: List[Any] = []
    for c in cols:
        v = fields.get(c)
        params += [v, v, v]
    # city comes from the page too and is 0% filled from the listing
    extra = ""
    if fields.get("city"):
        extra = ", city=CASE WHEN ? IS NULL OR ?='' THEN city ELSE ? END"
        params += [fields["city"]] * 3
    params.append(time.time())
    params.append(int(university_id))
    with connect(db_path) as conn:
        conn.execute(
            f"UPDATE sa_universities SET {sets}{extra}, detail_scraped_at=? "
            f"WHERE university_id=?", params)


def upsert_university_rankings(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows if r.get("university_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_university_rankings", _URANK_COLS,
                       ["university_id", "agency_id", "year", "stream"], rows)


def upsert_university_costs(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows if r.get("university_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_university_costs", _UCOST_COLS,
                       ["university_id", "kind", "label"], rows)


def upsert_university_nearby(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows if r.get("university_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_university_nearby", _UNEAR_COLS,
                       ["university_id", "category", "name"], rows)


def upsert_university_courses(rows, db_path: str = SA_DB_PATH) -> int:
    rows = [r for r in rows
            if r.get("university_id") is not None and r.get("course_id") is not None]
    with connect(db_path) as conn:
        return _upsert(conn, "sa_university_courses", _UCOURSE_COLS,
                       ["university_id", "course_id"], rows, preserve_nonempty=True)


def fill_program_exam_scores(rows, db_path: str = SA_DB_PATH) -> int:
    """Fill sa_program_exams.out_of / exam_score from the university page.

    The programme LISTING sends exam_out_of_score blank — which is why `out_of`
    is 0% filled across 314k rows. The university page sends it populated. This
    only ever fills a blank; it never overwrites a stored value."""
    n = 0
    with connect(db_path) as conn:
        for r in rows:
            pid, sf = r.get("program_id"), r.get("short_form")
            if pid is None or not sf:
                continue
            cur = conn.execute(
                "UPDATE sa_program_exams SET "
                "out_of=CASE WHEN COALESCE(out_of,'')='' THEN ? ELSE out_of END, "
                "exam_score=CASE WHEN COALESCE(exam_score,'')='' THEN ? ELSE exam_score END "
                "WHERE program_id=? AND short_form=?",
                (r.get("out_of") or "", r.get("exam_score") or "", pid, sf))
            n += cur.rowcount or 0
    return n


def set_university_progress(university_id: int, status: str, courses: int = 0,
                            rankings: int = 0, db_path: str = SA_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sa_university_progress"
            "(university_id,status,found_courses,found_rankings,updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(university_id) DO UPDATE SET "
            "status=excluded.status,found_courses=excluded.found_courses,"
            "found_rankings=excluded.found_rankings,updated_at=excluded.updated_at",
            (int(university_id), status, int(courses), int(rankings), time.time()))


def universities_pending_detail(limit: int = 0, db_path: str = SA_DB_PATH):
    """Self-draining phase-④ queue: every university without a 'done' row."""
    sql = ("SELECT u.university_id, u.name, u.university_url FROM sa_universities u "
           "LEFT JOIN sa_university_progress p ON p.university_id=u.university_id "
           "WHERE COALESCE(u.university_url,'')<>'' "
           "AND (p.university_id IS NULL OR p.status<>'done') "
           "ORDER BY u.university_id")
    if limit:
        sql += f" LIMIT {int(limit)}"
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql)]


STAGE_PK = {
    "sa_programs": lambda r: str(r.get("program_id")),
    "sa_universities": lambda r: str(r.get("university_id")),
    "sa_countries": lambda r: str(r.get("country_code")),
    "sa_program_exams": lambda r: f"{r.get('program_id')}|{r.get('short_form')}",
    "sa_scholarships": lambda r: str(r.get("scholarship_id")),
}
_UPSERTERS = {
    "sa_programs": upsert_programs,
    "sa_universities": upsert_universities,
    "sa_countries": upsert_countries,
    "sa_program_exams": upsert_program_exams,
    "sa_scholarships": upsert_scholarships,
}
PROMOTE_CHUNK = int(os.environ.get("CD_SA_PROMOTE_CHUNK", "2500"))


def stage_records(job_id: int, table: str, rows, db_path: str = SA_DB_PATH) -> int:
    rows = list(rows)
    if not rows:
        return 0
    pkf = STAGE_PK[table]
    now = time.time()
    with connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO sa_staging(job_id,table_name,pk,payload,staged_at) VALUES(?,?,?,?,?) "
            "ON CONFLICT(job_id,table_name,pk) DO UPDATE SET payload=excluded.payload",
            [(job_id, table, pkf(r), json.dumps(r), now) for r in rows])
    return len(rows)


def write_rows(job_id: int, cfg: Dict[str, Any], table: str, rows) -> int:
    """Route a runner's output: to staging (governance, default) or straight to
    master when cfg['staging'] is False. Mirrors the domestic _write_rows."""
    rows = list(rows)
    if not rows:
        return 0
    if cfg.get("staging", True):
        return stage_records(job_id, table, rows)
    return _UPSERTERS[table](rows)


def staged_summary(job_id: int, db_path: str = SA_DB_PATH) -> Dict[str, int]:
    with connect(db_path) as conn:
        return {r[0]: r[1] for r in conn.execute(
            "SELECT table_name, COUNT(*) FROM sa_staging WHERE job_id=? GROUP BY table_name",
            (job_id,)).fetchall()}


def flush_job_staging(job_id: int, chunk: int = PROMOTE_CHUNK,
                      db_path: str = SA_DB_PATH) -> Dict[str, int]:
    """Promote staged rows to master in MEMORY-SAFE chunks, deleting each chunk as
    it goes (incremental promotion). Returns {table: promoted}."""
    summary: Dict[str, int] = {}
    while True:
        with connect(db_path) as conn:
            batch = conn.execute(
                "SELECT id, table_name, payload FROM sa_staging WHERE job_id=? "
                "ORDER BY id LIMIT ?", (job_id, int(chunk))).fetchall()
        if not batch:
            break
        by: Dict[str, list] = {}
        ids = []
        for r in batch:
            ids.append(r["id"])
            try:
                by.setdefault(r["table_name"], []).append(json.loads(r["payload"]))
            except (json.JSONDecodeError, TypeError):
                continue
        for tbl, rows in by.items():
            _UPSERTERS[tbl](rows, db_path=db_path)
            summary[tbl] = summary.get(tbl, 0) + len(rows)
        with connect(db_path) as conn:
            conn.execute(f"DELETE FROM sa_staging WHERE id IN ({','.join('?' * len(ids))})", ids)
    return summary


def validate_job(job_id: int, rules: Optional[Dict[str, Any]] = None,
                 db_path: str = SA_DB_PATH) -> Dict[str, Any]:
    """QC the staged programs: enough rows? too many missing fees? Returns a
    score/pass flag mirroring the domestic validator."""
    rules = rules or {}
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT payload FROM sa_staging WHERE job_id=? AND table_name='sa_programs'",
            (job_id,)).fetchall()
    progs = len(rows)
    missing = 0
    for r in rows:
        try:
            if not json.loads(r[0]).get("fee_native_amount"):
                missing += 1
        except Exception:  # noqa: BLE001
            pass
    miss_pct = (100.0 * missing / progs) if progs else 0.0
    score = 100.0
    if progs < int(rules.get("min_rows", 1)):
        score -= 50
    if miss_pct > float(rules.get("max_missing_fee_pct", 100)):
        score -= 30
    passed = score >= float(rules.get("pass_score", 70))
    return {"score": score, "passed": passed, "programs": progs,
            "missing_fee_pct": round(miss_pct, 1)}


def promote_job(job_id: int, db_path: str = SA_DB_PATH) -> Dict[str, int]:
    return flush_job_staging(job_id, db_path=db_path)


def discard_staging(job_id: int, db_path: str = SA_DB_PATH) -> int:
    with connect(db_path) as conn:
        return conn.execute("DELETE FROM sa_staging WHERE job_id=?", (job_id,)).rowcount


def staging_count(db_path: str = SA_DB_PATH) -> int:
    with connect(db_path) as conn:
        return conn.execute("SELECT COUNT(*) FROM sa_staging").fetchone()[0]


def discard_all_staging(db_path: str = SA_DB_PATH) -> int:
    with connect(db_path) as conn:
        return conn.execute("DELETE FROM sa_staging").rowcount


# ---------------------------------------------------------------------------
# Ops: reset / logs / snapshots (SA-scoped, never touch domestic tables)
# ---------------------------------------------------------------------------
def clear_logs(job_id: Optional[int] = None, db_path: str = SA_DB_PATH) -> int:
    with connect(db_path) as conn:
        if job_id:
            return conn.execute("DELETE FROM sa_logs WHERE job_id=?", (job_id,)).rowcount
        return conn.execute("DELETE FROM sa_logs").rowcount


def prune_logs(keep: int = 8000, db_path: str = SA_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("DELETE FROM sa_logs WHERE id NOT IN "
                     "(SELECT id FROM sa_logs ORDER BY id DESC LIMIT ?)", (keep,))


def wipe_sa(full: bool = False, db_path: str = SA_DB_PATH) -> Dict[str, int]:
    """Delete ONLY sa_ data. Default wipes the dataset (programs/universities/
    countries/exams/progress/staging); full=True also clears jobs/logs/facets/
    snapshots. Never touches domestic tables."""
    tables = ["sa_programs", "sa_universities", "sa_countries", "sa_program_exams",
              "sa_scholarships", "sa_program_progress", "sa_staging",
              "sa_university_rankings", "sa_university_costs",
              "sa_university_nearby", "sa_university_courses",
              "sa_university_progress"]
    if full:
        tables += ["sa_jobs", "sa_logs", "sa_facets", "sa_snapshots"]
    out: Dict[str, int] = {}
    with connect(db_path) as conn:
        for t in tables:
            try:
                out[t] = conn.execute(f"DELETE FROM {t}").rowcount
            except Exception:  # noqa: BLE001
                pass
    if full:
        set_setting("facets_captured", False, db_path)
    return out


def add_snapshot(note: str = "", db_path: str = SA_DB_PATH) -> None:
    c = counts(db_path)
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sa_snapshots(ts,programs,universities,countries,program_exams,note) "
            "VALUES(?,?,?,?,?,?)",
            (time.time(), c["programs"], c["universities"], c["countries"],
             c["program_exams"], note))


def get_snapshots(limit: int = 100, db_path: str = SA_DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM sa_snapshots ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]


# ---------------------------------------------------------------------------
# Progress (resume)
# ---------------------------------------------------------------------------
def set_progress(partition_key: str, status: str, last_page: int, found: int,
                 db_path: str = SA_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sa_program_progress(partition_key,status,last_page,found,updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(partition_key) DO UPDATE SET "
            "status=excluded.status,last_page=excluded.last_page,found=excluded.found,"
            "updated_at=excluded.updated_at",
            (partition_key, status, last_page, found, time.time()))


def get_progress(partition_key: str, db_path: str = SA_DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        r = conn.execute("SELECT * FROM sa_program_progress WHERE partition_key=?",
                         (partition_key,)).fetchone()
    return dict(r) if r else None


def done_partitions(db_path: str = SA_DB_PATH) -> set:
    with connect(db_path) as conn:
        return {r[0] for r in conn.execute(
            "SELECT partition_key FROM sa_program_progress WHERE status='done'")}


# ---------------------------------------------------------------------------
# Jobs / logs (isolated)
# ---------------------------------------------------------------------------
def create_job(phase: str, config: Dict[str, Any], db_path: str = SA_DB_PATH) -> int:
    now = time.time()
    with connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO sa_jobs(vertical,phase,status,config_json,started_at,updated_at) "
            "VALUES('studyabroad',?,?,?,?,?)",
            (phase, "queued", json.dumps(_redact(config)), now, now))
        return cur.lastrowid


def update_job(job_id: int, db_path: str = SA_DB_PATH, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    sets = ",".join(f"{k}=?" for k in fields)
    with connect(db_path) as conn:
        conn.execute(f"UPDATE sa_jobs SET {sets} WHERE id=?",
                     (*fields.values(), job_id))


def get_job(job_id: int, db_path: str = SA_DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        r = conn.execute("SELECT * FROM sa_jobs WHERE id=?", (job_id,)).fetchone()
    return dict(r) if r else None


def list_jobs(limit: int = 30, db_path: str = SA_DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM sa_jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]


def request_stop(job_id: int, db_path: str = SA_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("UPDATE sa_jobs SET stop_requested=1 WHERE id=?", (job_id,))


def stop_requested(job_id: int, db_path: str = SA_DB_PATH) -> bool:
    with connect(db_path) as conn:
        r = conn.execute("SELECT stop_requested FROM sa_jobs WHERE id=?", (job_id,)).fetchone()
    return bool(r and r[0])


def add_log(job_id: int, message: str, db_path: str = SA_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("INSERT INTO sa_logs(job_id,ts,message) VALUES(?,?,?)",
                     (job_id, time.time(), message))


def get_logs(job_id: int, limit: int = 200, db_path: str = SA_DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM sa_logs WHERE job_id=? ORDER BY id DESC LIMIT ?",
            (job_id, limit)).fetchall()]


def get_setting(key: str, default=None, db_path: str = SA_DB_PATH):
    with connect(db_path) as conn:
        r = conn.execute("SELECT value FROM sa_settings WHERE key=?", (key,)).fetchone()
    if not r:
        return default
    try:
        return json.loads(r[0])
    except Exception:  # noqa: BLE001
        return r[0]


def set_setting(key: str, value, db_path: str = SA_DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("INSERT INTO sa_settings(key,value) VALUES(?,?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                     (key, json.dumps(value)))


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def counts(db_path: str = SA_DB_PATH) -> Dict[str, int]:
    with connect(db_path) as conn:
        def one(q):
            try:
                return conn.execute(q).fetchone()[0]
            except Exception:  # noqa: BLE001
                return 0
        return {
            "programs": one("SELECT COUNT(*) FROM sa_programs"),
            "universities": one("SELECT COUNT(*) FROM sa_universities"),
            "countries": one("SELECT COUNT(*) FROM sa_countries"),
            "universities_detailed": one("SELECT COUNT(*) FROM sa_universities "
                                         "WHERE detail_scraped_at IS NOT NULL"),
            "university_rankings": one("SELECT COUNT(*) FROM sa_university_rankings"),
            "university_courses": one("SELECT COUNT(*) FROM sa_university_courses"),
            "university_nearby": one("SELECT COUNT(*) FROM sa_university_nearby"),
            "program_exams": one("SELECT COUNT(*) FROM sa_program_exams"),
            "programs_with_url": one("SELECT COUNT(*) FROM sa_programs WHERE program_url<>''"),
            "programs_with_fee": one("SELECT COUNT(*) FROM sa_programs WHERE fee_native_amount IS NOT NULL"),
            "distinct_currencies": one("SELECT COUNT(DISTINCT fee_currency) FROM sa_programs WHERE fee_currency<>''"),
            "partitions_done": one("SELECT COUNT(*) FROM sa_program_progress WHERE status='done'"),
            "scholarships": one("SELECT COUNT(*) FROM sa_scholarships"),
            "scholarships_detailed": one("SELECT COUNT(*) FROM sa_scholarships WHERE detail_scraped_at IS NOT NULL"),
        }


if __name__ == "__main__":
    init_db()
    print(f"SA DB initialised at {SA_DB_PATH}")
    print(counts())
