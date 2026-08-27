"""
SQLite data layer for the Collegedunia scraper.

Three core tables model the many-to-many reality of the site:

    courses     - the ~21,500 courses (phase 1)
    colleges    - unique colleges, deduplicated by college_id (filled in phase 2)
    offerings   - the junction: one row per course offered at a college, carrying
                  fees / ranking / cutoff / rating / admission dates (phase 2)

Plus bookkeeping tables:

    offering_progress - per-course phase-2 progress so it can resume
    jobs              - one row per scrape run (status, counters, stop flag)
    settings          - persisted UI settings (proxies, delay, etc.)

WAL mode is enabled so the Streamlit UI can read while the worker writes.
"""

from __future__ import annotations

BUILD = "2026-07-23a"  # keep in sync across app/db/scraper/export (header checks this)

import json
import os
import re
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional


def fee_to_inr(value: Any) -> Optional[int]:
    """Normalise mixed fee strings to an integer INR amount.
    '2.65 Lakhs' -> 265000, '₹894' -> 894, '4.46 L' -> 446000,
    '₹ 6,000' -> 6000, '1.2 Cr' -> 12000000. Returns None if unparseable."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    t = str(value).lower().replace(",", "").replace("₹", "").replace("inr", "").strip()
    m = re.search(r"([\d.]+)\s*(lakhs?|crores?|cr|l|k|thousand)?", t)
    if not m:
        return None
    try:
        v = float(m.group(1))
    except ValueError:
        return None
    unit = (m.group(2) or "").strip()
    if unit in ("lakh", "lakhs", "l"):
        v *= 100000
    elif unit in ("crore", "crores", "cr"):
        v *= 10000000
    elif unit in ("k", "thousand"):
        v *= 1000
    return int(round(v))

DB_PATH = os.environ.get("CD_DB_PATH", os.path.join(os.path.dirname(__file__), "data.db"))

# Rows held in RAM per promotion batch. Sized for the host: 2500 is comfortable
# on a 2 GB box and drains a big staging backlog in far fewer passes; drop to
# ~500 on a 512 MB host. Override with CD_PROMOTE_CHUNK.
PROMOTE_CHUNK = int(os.environ.get("CD_PROMOTE_CHUNK", "2500"))

# Collegedunia internal stream IDs -> human-readable names (verified via the API).
STREAMS = {
    1: "Agriculture", 2: "Architecture", 3: "Arts", 4: "Aviation",
    5: "Commerce", 6: "Computer Applications", 7: "Dental", 8: "Design",
    9: "Education", 10: "Engineering", 11: "Hotel Management", 12: "Law",
    13: "Management", 14: "Mass Communications", 15: "Medical",
    16: "Paramedical", 17: "Pharmacy", 18: "Science",
    19: "Veterinary Sciences", 20: "Vocational Courses",
}


def stream_name(sid: Any) -> str:
    try:
        return STREAMS.get(int(sid), f"Stream {sid}")
    except (TypeError, ValueError):
        return ""


@contextmanager
def connect(db_path: str = DB_PATH) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(db_path, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=60000;")
        yield conn
        conn.commit()
    finally:
        conn.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS courses (
    course_id      INTEGER PRIMARY KEY,
    name           TEXT,
    course_link    TEXT,
    listing_link   TEXT,
    duration       TEXT,
    course_type    TEXT,
    level          TEXT,
    eligibility    TEXT,
    program_type   TEXT,
    mode           TEXT,
    exam_name      TEXT,
    exam_url       TEXT,
    fees           TEXT,
    avg_salary     TEXT,
    colleges_count INTEGER,
    job_roles      TEXT,
    topics_covered TEXT,
    stream_id      TEXT,
    stream_name    TEXT,
    course_tag     TEXT,
    course_tag_id  TEXT,
    description    TEXT,
    colleges_url   TEXT,
    raw_json       TEXT,
    scraped_at     REAL
);

CREATE TABLE IF NOT EXISTS colleges (
    college_id   INTEGER PRIMARY KEY,
    name         TEXT,
    short_form   TEXT,
    city         TEXT,
    state_id     INTEGER,
    link         TEXT,
    logo         TEXT,
    raw_json     TEXT,
    website      TEXT,
    email        TEXT,
    phone        TEXT,
    rating_value REAL,
    rating_count INTEGER,
    pros         TEXT,
    cons         TEXT,
    address      TEXT,
    enriched_at  REAL,
    scraped_at   REAL
);

CREATE TABLE IF NOT EXISTS offerings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id       INTEGER,
    college_id      INTEGER,
    course_name     TEXT,
    course_acronym  TEXT,
    college_name    TEXT,
    college_short   TEXT,
    city            TEXT,
    state_id        INTEGER,
    logo            TEXT,
    fees_amount     INTEGER,
    fees_text       TEXT,
    eligibility     TEXT,
    exam_name       TEXT,
    exam_url        TEXT,
    duration        TEXT,
    course_type     TEXT,
    level           TEXT,
    program_type    TEXT,
    mode            TEXT,
    ranking_rank    INTEGER,
    ranking_agency  TEXT,
    ranking_total   INTEGER,
    ranking_stream  TEXT,
    ranking_url     TEXT,
    course_rating   REAL,
    reviews_count   INTEGER,
    major_stream_rating REAL,
    cutoff_exam     TEXT,
    cutoff_value    INTEGER,
    admission_start TEXT,
    admission_end   TEXT,
    job_roles       TEXT,
    topics_covered  TEXT,
    description     TEXT,
    stream_id       TEXT,
    course_tag      TEXT,
    course_tag_id   TEXT,
    university_link TEXT,
    raw_json        TEXT,
    scraped_at      REAL,
    UNIQUE(course_id, college_id)
);

CREATE TABLE IF NOT EXISTS offering_progress (
    course_id   INTEGER PRIMARY KEY,
    status      TEXT,
    pages_done  INTEGER DEFAULT 0,
    total_count INTEGER,
    updated_at  REAL
);

CREATE TABLE IF NOT EXISTS jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    type           TEXT,
    status         TEXT,
    config_json    TEXT,
    total_units    INTEGER DEFAULT 0,
    done_units     INTEGER DEFAULT 0,
    items_written  INTEGER DEFAULT 0,
    req_count      INTEGER DEFAULT 0,
    bytes_count    INTEGER DEFAULT 0,
    message        TEXT,
    stop_requested INTEGER DEFAULT 0,
    pid            INTEGER,
    started_at     REAL,
    updated_at     REAL,
    finished_at    REAL
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);

-- Phase 4: per-college courses & fees (scraped from /college/<id>/courses-fees)
CREATE TABLE IF NOT EXISTS college_courses (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    college_id   INTEGER,
    college_name TEXT,
    course_name  TEXT,
    eligibility  TEXT,
    total_fees   TEXT,
    hostel_fees  TEXT,
    source_url   TEXT,
    scraped_at   REAL,
    UNIQUE(college_id, course_name)
);

CREATE TABLE IF NOT EXISTS cc_progress (
    college_id INTEGER PRIMARY KEY,
    status     TEXT,            -- 'done' | 'empty' | 'error'
    found      INTEGER DEFAULT 0,
    updated_at REAL
);

CREATE TABLE IF NOT EXISTS logs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id  INTEGER,
    ts      REAL,
    message TEXT
);

-- Per-job staging: every job writes here first; promoted to master only after QC.
CREATE TABLE IF NOT EXISTS staging (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     INTEGER,
    table_name TEXT,
    pk         TEXT,
    payload    TEXT,
    staged_at  REAL,
    UNIQUE(job_id, table_name, pk)
);

-- Directory phase: the complete india-colleges directory (coverage baseline).
CREATE TABLE IF NOT EXISTS colleges_directory (
    college_id      INTEGER PRIMARY KEY,
    name            TEXT,
    short_form      TEXT,
    city            TEXT,
    city_id         INTEGER,
    state           TEXT,
    state_id        INTEGER,
    link            TEXT,
    rating          REAL,
    naac_grading    TEXT,
    top_course_fees TEXT,
    course_count    INTEGER,
    approvals       TEXT,
    source_slug     TEXT,
    raw_json        TEXT,
    scraped_at      REAL
);

-- Per-partition (state slug / base sweep) progress for Directory resume.
CREATE TABLE IF NOT EXISTS dir_progress (
    slug       TEXT PRIMARY KEY,
    status     TEXT,            -- 'partial' | 'done'
    found      INTEGER DEFAULT 0,
    last_page  INTEGER DEFAULT 0,
    updated_at REAL
);

-- Enrichment A: per-course aggregates DERIVED from already-scraped offerings
-- (no network). Rebuilt on demand; keyed by course_id.
CREATE TABLE IF NOT EXISTS course_enrichment (
    course_id    INTEGER PRIMARY KEY,
    n_colleges   INTEGER DEFAULT 0,
    n_cities     INTEGER DEFAULT 0,
    n_states     INTEGER DEFAULT 0,
    fee_min      INTEGER,
    fee_avg      INTEGER,
    fee_max      INTEGER,
    avg_rating   REAL,
    top_colleges TEXT,          -- JSON: [{college, city, fee, rank}]
    state_spread TEXT,          -- JSON: {state_id: college_count}
    updated_at   REAL
);

CREATE INDEX IF NOT EXISTS idx_offerings_course ON offerings(course_id);
CREATE INDEX IF NOT EXISTS idx_offerings_college ON offerings(college_id);
CREATE INDEX IF NOT EXISTS idx_cc_college ON college_courses(college_id);
CREATE INDEX IF NOT EXISTS idx_logs_job ON logs(job_id, id);
CREATE INDEX IF NOT EXISTS idx_staging_job ON staging(job_id, table_name);
"""


def init_db(db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        # Migration: add stream_name to existing course tables + backfill.
        cols = {r[1] for r in conn.execute("PRAGMA table_info(courses)")}
        if "stream_name" not in cols:
            conn.execute("ALTER TABLE courses ADD COLUMN stream_name TEXT")
        for sid, name in STREAMS.items():
            conn.execute(
                "UPDATE courses SET stream_name=? WHERE stream_id=? AND "
                "(stream_name IS NULL OR stream_name='')",
                (name, str(sid)),
            )
        # Migration: job bandwidth/request counters.
        jcols = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
        if "req_count" not in jcols:
            conn.execute("ALTER TABLE jobs ADD COLUMN req_count INTEGER DEFAULT 0")
        if "bytes_count" not in jcols:
            conn.execute("ALTER TABLE jobs ADD COLUMN bytes_count INTEGER DEFAULT 0")
        # Migration: Phase-3 college enrichment columns.
        kcols = {r[1] for r in conn.execute("PRAGMA table_info(colleges)")}
        for col, typ in (("website", "TEXT"), ("email", "TEXT"), ("phone", "TEXT"),
                         ("rating_value", "REAL"), ("rating_count", "INTEGER"),
                         ("pros", "TEXT"), ("cons", "TEXT"), ("address", "TEXT"),
                         ("enriched_at", "REAL")):
            if col not in kcols:
                conn.execute(f"ALTER TABLE colleges ADD COLUMN {col} {typ}")
        # Migration: numeric fee + rich Phase-4 columns for college_courses.
        cccols = {r[1] for r in conn.execute("PRAGMA table_info(college_courses)")}
        for col, typ in (("fees_inr", "INTEGER"), ("specialization", "TEXT"),
                         ("duration", "TEXT"), ("mode", "TEXT"), ("level", "TEXT"),
                         ("course_type", "TEXT"), ("rating", "REAL"),
                         ("reviews_count", "INTEGER"), ("application_start", "TEXT"),
                         ("application_end", "TEXT"), ("course_url", "TEXT")):
            if col not in cccols:
                conn.execute(f"ALTER TABLE college_courses ADD COLUMN {col} {typ}")
        # Migration: last completed course_page for Phase-4 mid-college resume.
        ccpcols = {r[1] for r in conn.execute("PRAGMA table_info(cc_progress)")}
        if "last_page" not in ccpcols:
            conn.execute("ALTER TABLE cc_progress ADD COLUMN last_page INTEGER DEFAULT 0")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshots ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, "
            "courses INTEGER, colleges INTEGER, offerings INTEGER, "
            "college_courses INTEGER, note TEXT)")
        # Migration: provenance column on master tables.
        for tbl in ("courses", "colleges", "offerings", "college_courses",
                    "colleges_directory"):
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")}
            if "source_job_id" not in cols:
                conn.execute(f"ALTER TABLE {tbl} ADD COLUMN source_job_id INTEGER")
        # Migration: governance columns on jobs.
        jcols2 = {r[1] for r in conn.execute("PRAGMA table_info(jobs)")}
        for col, typ in (("quality_score", "REAL"), ("promote_status", "TEXT"),
                         ("staged_rows", "INTEGER")):
            if col not in jcols2:
                conn.execute(f"ALTER TABLE jobs ADD COLUMN {col} {typ}")


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------
def get_setting(key: str, default: Any = None, db_path: str = DB_PATH) -> Any:
    with connect(db_path) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    if row is None:
        return default
    try:
        return json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        return row["value"]


def set_setting(key: str, value: Any, db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )


# ---------------------------------------------------------------------------
# Course upserts (phase 1)
# ---------------------------------------------------------------------------
def upsert_courses(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH,
                   fill_empty: bool = False) -> int:
    """Insert/refresh course rows. Normally overwrites every column on conflict.
    With fill_empty=True (Enrichment B — backfill mode) an existing non-empty
    value is preserved and only NULL/'' columns are patched from the new scrape,
    so a re-run can top up gaps without clobbering good data. course_id, raw_json,
    scraped_at and source_job_id are always refreshed for provenance."""
    rows = list(rows)
    if not rows:
        return 0
    cols = [
        "course_id", "name", "course_link", "listing_link", "duration", "course_type",
        "level", "eligibility", "program_type", "mode", "exam_name", "exam_url",
        "fees", "avg_salary", "colleges_count", "job_roles", "topics_covered",
        "stream_id", "stream_name", "course_tag", "course_tag_id", "description",
        "colleges_url", "raw_json", "scraped_at", "source_job_id",
    ]
    always = {"course_id", "raw_json", "scraped_at", "source_job_id"}
    placeholders = ",".join("?" for _ in cols)
    if fill_empty:
        sets = []
        for c in cols:
            if c == "course_id":
                continue
            if c in always:
                sets.append(f"{c}=excluded.{c}")
            else:
                sets.append(
                    f"{c}=CASE WHEN courses.{c} IS NULL OR courses.{c}='' "
                    f"THEN excluded.{c} ELSE courses.{c} END")
        set_clause = ",".join(sets)
    else:
        set_clause = ",".join(f"{c}=excluded.{c}" for c in cols if c != "course_id")
    sql = (
        f"INSERT INTO courses ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(course_id) DO UPDATE SET " + set_clause
    )
    with connect(db_path) as conn:
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


# ---------------------------------------------------------------------------
# College + offering upserts (phase 2)
# ---------------------------------------------------------------------------
def upsert_colleges(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
    rows = list(rows)
    if not rows:
        return 0
    cols = ["college_id", "name", "short_form", "city", "state_id", "link", "logo",
            "raw_json", "scraped_at", "source_job_id"]
    placeholders = ",".join("?" for _ in cols)
    sql = (
        f"INSERT INTO colleges ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(college_id) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}" for c in cols if c != "college_id")
    )
    with connect(db_path) as conn:
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


def upsert_offerings(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
    rows = list(rows)
    if not rows:
        return 0
    cols = [
        "course_id", "college_id", "course_name", "course_acronym", "college_name",
        "college_short", "city", "state_id", "logo", "fees_amount", "fees_text",
        "eligibility", "exam_name", "exam_url", "duration", "course_type", "level",
        "program_type", "mode", "ranking_rank", "ranking_agency", "ranking_total",
        "ranking_stream", "ranking_url", "course_rating", "reviews_count",
        "major_stream_rating", "cutoff_exam", "cutoff_value", "admission_start",
        "admission_end", "job_roles", "topics_covered", "description", "stream_id",
        "course_tag", "course_tag_id", "university_link", "raw_json", "scraped_at",
        "source_job_id",
    ]
    placeholders = ",".join("?" for _ in cols)
    sql = (
        f"INSERT INTO offerings ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(course_id, college_id) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("course_id", "college_id"))
    )
    with connect(db_path) as conn:
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


# ---------------------------------------------------------------------------
# Offering progress (phase-2 resume)
# ---------------------------------------------------------------------------
def set_offering_progress(course_id: int, status: str, pages_done: int,
                          total_count: Optional[int], db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO offering_progress(course_id, status, pages_done, total_count, updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(course_id) DO UPDATE SET "
            "status=excluded.status, pages_done=excluded.pages_done, "
            "total_count=excluded.total_count, updated_at=excluded.updated_at",
            (course_id, status, pages_done, total_count, time.time()),
        )


def get_done_course_ids(db_path: str = DB_PATH) -> set:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT course_id FROM offering_progress WHERE status='done'"
        ).fetchall()
    return {r["course_id"] for r in rows}


def list_colleges_to_enrich(db_path: str = DB_PATH, where: str = "", params: tuple = (),
                            include_done: bool = False, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    sql = "SELECT college_id, link FROM colleges"
    conds = []
    if where:
        conds.append(where)
    if not include_done:
        conds.append("(enriched_at IS NULL)")
    conds.append("link IS NOT NULL AND link<>''")
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY college_id"
    if limit:
        sql += f" LIMIT {int(limit)}"
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def update_college_details(college_id: int, fields: Dict[str, Any], db_path: str = DB_PATH) -> None:
    """Write Phase-3 enrichment onto a college row.

    NON-DESTRUCTIVE: a blank/NULL incoming value never overwrites an existing
    non-empty one. A re-run against a page that has lost its JSON-LD (or a
    partially-parsed one) can therefore only ever add detail, never erase it.
    enriched_at is always refreshed so the college leaves the pending queue."""
    cols = ["website", "email", "phone", "rating_value", "rating_count",
            "pros", "cons", "address"]
    sets = ", ".join(
        f"{c}=CASE WHEN ? IS NULL OR ?='' THEN {c} ELSE ? END" for c in cols
    ) + ", enriched_at=?"
    vals: List[Any] = []
    for c in cols:
        v = fields.get(c)
        vals += [v, v, v]
    vals += [time.time(), college_id]
    with connect(db_path) as conn:
        conn.execute(f"UPDATE colleges SET {sets} WHERE college_id=?", vals)


def normalize_fees(db_path: str = DB_PATH) -> int:
    """Backfill college_courses.fees_inr from the total_fees string."""
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, total_fees FROM college_courses "
            "WHERE fees_inr IS NULL AND total_fees IS NOT NULL AND total_fees<>''").fetchall()
        n = 0
        for r in rows:
            v = fee_to_inr(r["total_fees"])
            if v is not None:
                conn.execute("UPDATE college_courses SET fees_inr=? WHERE id=?", (v, r["id"]))
                n += 1
    return n


def add_snapshot(note: str = "", db_path: str = DB_PATH) -> None:
    c = counts(db_path=db_path)
    with connect(db_path) as conn:
        cc = conn.execute("SELECT COUNT(*) FROM college_courses").fetchone()[0]
        conn.execute(
            "INSERT INTO snapshots(ts, courses, colleges, offerings, college_courses, note) "
            "VALUES(?,?,?,?,?,?)",
            (time.time(), c["courses"], c["colleges"], c["offerings"], cc, note))


def get_snapshots(limit: int = 50, db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM snapshots ORDER BY id DESC LIMIT ?", (limit,)).fetchall()]


def qa_report(db_path: str = DB_PATH) -> Dict[str, int]:
    """Quick data-health counts."""
    out: Dict[str, int] = {}
    with connect(db_path) as conn:
        def one(q):
            try:
                return conn.execute(q).fetchone()[0]
            except Exception:
                return 0
        out["courses_zero_colleges"] = one("SELECT COUNT(*) FROM courses WHERE colleges_count=0 OR colleges_count IS NULL")
        out["offerings_no_fee"] = one("SELECT COUNT(*) FROM offerings WHERE fees_amount IS NULL OR fees_amount=0")
        out["offerings_no_rating"] = one("SELECT COUNT(*) FROM offerings WHERE course_rating IS NULL OR course_rating=0")
        out["colleges_no_city"] = one("SELECT COUNT(*) FROM colleges WHERE city IS NULL OR city=''")
        out["colleges_unenriched"] = one("SELECT COUNT(*) FROM colleges WHERE enriched_at IS NULL")
        out["dup_college_names"] = one(
            "SELECT COUNT(*) FROM (SELECT name FROM colleges WHERE name<>'' GROUP BY name HAVING COUNT(*)>1)")
        out["cc_unparsed_fees"] = one(
            "SELECT COUNT(*) FROM college_courses WHERE (fees_inr IS NULL) AND total_fees<>''")
    return out


# college_courses rows whose course_name is clearly junk (leftovers from the old
# HTML-table parser: fee labels, amounts, or college names leaked as a course).
_JUNK_CC_WHERE = (
    "(LOWER(course_name) LIKE '%fee%' "
    "OR (LOWER(course_name) LIKE '%college%' AND course_name LIKE '%,%') "
    "OR (LOWER(course_name) LIKE '%university%' AND course_name LIKE '%,%') "
    "OR substr(course_name,1,1) IN ('₹','0','1','2','3','4','5','6','7','8','9') "
    "OR LOWER(TRIM(course_name)) IN "
    "('semester','amount','total','tuition','yearly','caution','admission',"
    "'registration','exam','other','hostel','n/a','na','-',''))"
)


def count_junk_college_courses(db_path: str = DB_PATH) -> int:
    with connect(db_path) as conn:
        return conn.execute(
            f"SELECT COUNT(*) FROM college_courses WHERE {_JUNK_CC_WHERE}").fetchone()[0]


def sample_junk_college_courses(limit: int = 50, db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            f"SELECT college_id, college_name, course_name, total_fees "
            f"FROM college_courses WHERE {_JUNK_CC_WHERE} LIMIT ?", (int(limit),)).fetchall()]


def delete_junk_college_courses(db_path: str = DB_PATH) -> int:
    with connect(db_path) as conn:
        return conn.execute(f"DELETE FROM college_courses WHERE {_JUNK_CC_WHERE}").rowcount


def upsert_college_courses(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
    rows = [r for r in rows if r.get("course_name")]
    if not rows:
        return 0
    cols = ["college_id", "college_name", "course_name", "specialization",
            "eligibility", "total_fees", "fees_inr", "hostel_fees", "duration",
            "mode", "level", "course_type", "rating", "reviews_count",
            "application_start", "application_end", "source_url", "course_url",
            "scraped_at", "source_job_id"]
    ph = ",".join("?" for _ in cols)
    sql = (f"INSERT INTO college_courses ({','.join(cols)}) VALUES ({ph}) "
           f"ON CONFLICT(college_id, course_name) DO UPDATE SET "
           + ",".join(f"{c}=excluded.{c}" for c in cols
                      if c not in ("college_id", "course_name")))
    with connect(db_path) as conn:
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


def set_cc_progress(college_id: int, status: str, found: int, last_page: int = 0,
                    db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO cc_progress(college_id, status, found, last_page, updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(college_id) DO UPDATE SET status=excluded.status, "
            "found=excluded.found, last_page=excluded.last_page, updated_at=excluded.updated_at",
            (college_id, status, found, int(last_page or 0), time.time()))


def get_cc_progress(college_id: int, db_path: str = DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM cc_progress WHERE college_id=?", (college_id,)).fetchone()
    return dict(row) if row else None


def get_cc_done_ids(db_path: str = DB_PATH) -> set:
    with connect(db_path) as conn:
        return {r[0] for r in conn.execute(
            "SELECT college_id FROM cc_progress WHERE status IN ('done','empty')")}


# ---------------------------------------------------------------------------
# Directory phase (india-colleges coverage baseline)
# ---------------------------------------------------------------------------
def upsert_colleges_directory(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
    rows = [r for r in rows if r.get("college_id") is not None]
    if not rows:
        return 0
    cols = ["college_id", "name", "short_form", "city", "city_id", "state",
            "state_id", "link", "rating", "naac_grading", "top_course_fees",
            "course_count", "approvals", "source_slug", "raw_json", "scraped_at",
            "source_job_id"]
    ph = ",".join("?" for _ in cols)
    sql = (f"INSERT INTO colleges_directory ({','.join(cols)}) VALUES ({ph}) "
           f"ON CONFLICT(college_id) DO UPDATE SET "
           + ",".join(f"{c}=excluded.{c}" for c in cols if c != "college_id"))
    with connect(db_path) as conn:
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


def set_dir_progress(slug: str, status: str, found: int, last_page: int = 0,
                     db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO dir_progress(slug, status, found, last_page, updated_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(slug) DO UPDATE SET status=excluded.status, "
            "found=excluded.found, last_page=excluded.last_page, updated_at=excluded.updated_at",
            (slug, status, int(found or 0), int(last_page or 0), time.time()))


def get_dir_progress(slug: str, db_path: str = DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM dir_progress WHERE slug=?", (slug,)).fetchone()
    return dict(row) if row else None


def get_dir_done_slugs(db_path: str = DB_PATH) -> set:
    with connect(db_path) as conn:
        return {r[0] for r in conn.execute("SELECT slug FROM dir_progress WHERE status='done'")}


def dir_missing_from_phase2(limit: int = 500000, db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    """Directory colleges NOT present in the Phase-2 colleges table (the gap)."""
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT d.college_id, d.name, d.city, d.state, d.state_id, d.link, "
            "d.course_count, d.source_slug FROM colleges_directory d "
            "LEFT JOIN colleges c ON c.college_id = d.college_id "
            "WHERE c.college_id IS NULL ORDER BY d.college_id LIMIT ?", (int(limit),)).fetchall()]


def dir_extra_not_in_directory(limit: int = 500000, db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    """Phase-2 colleges NOT present in the directory (usually fine — flag anyway)."""
    with connect(db_path) as conn:
        return [dict(r) for r in conn.execute(
            "SELECT c.college_id, c.name, c.city, c.state_id FROM colleges c "
            "LEFT JOIN colleges_directory d ON d.college_id = c.college_id "
            "WHERE d.college_id IS NULL ORDER BY c.college_id LIMIT ?", (int(limit),)).fetchall()]


def dir_coverage_summary(db_path: str = DB_PATH) -> Dict[str, Any]:
    with connect(db_path) as conn:
        dtot = conn.execute("SELECT COUNT(*) FROM colleges_directory").fetchone()[0]
        p2 = conn.execute("SELECT COUNT(*) FROM colleges").fetchone()[0]
        overlap = conn.execute(
            "SELECT COUNT(*) FROM colleges_directory d "
            "JOIN colleges c ON c.college_id = d.college_id").fetchone()[0]
        by_state = [dict(r) for r in conn.execute(
            "SELECT COALESCE(NULLIF(d.state,''),'?') AS state, COUNT(*) AS directory, "
            "SUM(CASE WHEN c.college_id IS NOT NULL THEN 1 ELSE 0 END) AS in_phase2, "
            "SUM(CASE WHEN c.college_id IS NULL THEN 1 ELSE 0 END) AS missing "
            "FROM colleges_directory d LEFT JOIN colleges c ON c.college_id = d.college_id "
            "GROUP BY d.state ORDER BY missing DESC").fetchall()]
    return {"directory_total": dtot, "phase2_total": p2, "overlap": overlap, "by_state": by_state}


def queue_missing_for_phase4(db_path: str = DB_PATH) -> int:
    """Insert directory colleges missing from Phase 2 into the Phase-4 work queue
    (cc_progress with status 'queued'), so their courses-fees pages get scraped.
    Leaves already-done/empty colleges untouched."""
    ids = [r["college_id"] for r in dir_missing_from_phase2(db_path=db_path)]
    if not ids:
        return 0
    with connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO cc_progress(college_id, status, found, last_page, updated_at) "
            "VALUES(?, 'queued', 0, 0, ?) ON CONFLICT(college_id) DO UPDATE SET "
            "status='queued' WHERE cc_progress.status NOT IN ('done','empty')",
            [(i, time.time()) for i in ids])
    return len(ids)


def list_cc_queued_ids(db_path: str = DB_PATH) -> List[int]:
    with connect(db_path) as conn:
        return [r[0] for r in conn.execute(
            "SELECT college_id FROM cc_progress WHERE status='queued'")]


def college_name_lookup(college_id: int, db_path: str = DB_PATH) -> str:
    """Best-effort college name from already-scraped tables (Phase 2 colleges,
    then the directory) — avoids fetching the heavy HTML page just for the name."""
    with connect(db_path) as conn:
        for tbl in ("colleges", "colleges_directory"):
            try:
                row = conn.execute(
                    f"SELECT name FROM {tbl} WHERE college_id=?", (college_id,)).fetchone()
                if row and row[0]:
                    return row[0]
            except Exception:  # noqa: BLE001
                pass
    return ""


def list_known_college_ids(db_path: str = DB_PATH) -> List[int]:
    with connect(db_path) as conn:
        return [r[0] for r in conn.execute(
            "SELECT college_id FROM colleges ORDER BY college_id")]


def list_colleges_missing_course_url(db_path: str = DB_PATH) -> List[int]:
    """College ids that still have >=1 college_courses row with no course_url.
    This IS the resumable backfill queue: each college drops out of the query
    the moment its rows are re-scraped and filled, so restarts auto-continue."""
    with connect(db_path) as conn:
        return [r[0] for r in conn.execute(
            "SELECT DISTINCT college_id FROM college_courses "
            "WHERE (course_url IS NULL OR course_url='') AND college_id IS NOT NULL "
            "ORDER BY college_id")]


def course_url_backfill_status(db_path: str = DB_PATH) -> Dict[str, int]:
    with connect(db_path) as conn:
        def one(q):
            return conn.execute(q).fetchone()[0]
        return {
            "rows_total": one("SELECT COUNT(*) FROM college_courses"),
            "rows_missing": one("SELECT COUNT(*) FROM college_courses "
                                "WHERE course_url IS NULL OR course_url=''"),
            "colleges_missing": one("SELECT COUNT(DISTINCT college_id) FROM college_courses "
                                    "WHERE course_url IS NULL OR course_url=''"),
        }


def list_directory_college_ids(db_path: str = DB_PATH) -> List[int]:
    """Every college_id discovered by the Directory phase (~18.8k) — the widest
    baseline. Feed this to Phase 4 to scrape courses & fees for colleges the
    course-finder (Phase 2) never surfaced. Resume/cc_progress skips done ones."""
    with connect(db_path) as conn:
        return [r[0] for r in conn.execute(
            "SELECT college_id FROM colleges_directory "
            "WHERE college_id IS NOT NULL ORDER BY college_id")]


def get_offering_progress(course_id: int, db_path: str = DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM offering_progress WHERE course_id=?", (course_id,)
        ).fetchone()
    return dict(row) if row else None


_ORDER_SQL = {
    "colleges_desc": "colleges_count DESC",
    "colleges_asc": "colleges_count ASC",
    "stream": "stream_id, colleges_count DESC",
}


def list_course_ids(db_path: str = DB_PATH, where: str = "", params: tuple = (),
                    order: str = "colleges_desc") -> List[int]:
    sql = "SELECT course_id FROM courses"
    if where:
        sql += f" WHERE {where}"
    sql += " ORDER BY " + _ORDER_SQL.get(order, "colleges_count DESC")
    with connect(db_path) as conn:
        return [r["course_id"] for r in conn.execute(sql, params).fetchall()]


# ---------------------------------------------------------------------------
# Enrichment A — derive per-course aggregates from already-scraped offerings.
# Pure SQL over data already in the DB: no network, memory-bounded (one small
# GROUP BY + a couple of tiny per-course lookups). Rebuilds course_enrichment.
# ---------------------------------------------------------------------------
def enrich_courses(db_path: str = DB_PATH) -> int:
    with connect(db_path) as conn:
        conn.execute("DELETE FROM course_enrichment")
        agg = conn.execute(
            "SELECT course_id, "
            "COUNT(DISTINCT college_id) AS n_colleges, "
            "COUNT(DISTINCT NULLIF(city,'')) AS n_cities, "
            "COUNT(DISTINCT state_id) AS n_states, "
            "MIN(NULLIF(fees_amount,0)) AS fee_min, "
            "CAST(AVG(NULLIF(fees_amount,0)) AS INTEGER) AS fee_avg, "
            "MAX(NULLIF(fees_amount,0)) AS fee_max, "
            "AVG(NULLIF(course_rating,0)) AS avg_rating "
            "FROM offerings GROUP BY course_id").fetchall()
        n = 0
        for r in agg:
            cid = r["course_id"]
            tops = conn.execute(
                "SELECT college_name, city, fees_amount, ranking_rank FROM offerings "
                "WHERE course_id=? AND college_name<>'' "
                "ORDER BY CASE WHEN ranking_rank>0 THEN ranking_rank ELSE 9999999 END, "
                "fees_amount DESC LIMIT 5", (cid,)).fetchall()
            top_json = json.dumps([
                {"college": t["college_name"], "city": t["city"],
                 "fee": t["fees_amount"], "rank": t["ranking_rank"]} for t in tops])
            spread = conn.execute(
                "SELECT CAST(state_id AS TEXT) AS s, COUNT(DISTINCT college_id) AS c "
                "FROM offerings WHERE course_id=? AND state_id IS NOT NULL "
                "GROUP BY state_id ORDER BY c DESC LIMIT 12", (cid,)).fetchall()
            spread_json = json.dumps({row["s"]: row["c"] for row in spread})
            conn.execute(
                "INSERT OR REPLACE INTO course_enrichment("
                "course_id, n_colleges, n_cities, n_states, fee_min, fee_avg, fee_max, "
                "avg_rating, top_colleges, state_spread, updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (cid, r["n_colleges"], r["n_cities"], r["n_states"],
                 r["fee_min"], r["fee_avg"], r["fee_max"],
                 round(r["avg_rating"], 2) if r["avg_rating"] else None,
                 top_json, spread_json, time.time()))
            n += 1
        return n


def course_enrichment_summary(db_path: str = DB_PATH) -> Dict[str, Any]:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n, MAX(updated_at) AS last FROM course_enrichment").fetchone()
    return {"rows": row["n"] if row else 0, "last": row["last"] if row else None}


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------
def create_job(job_type: str, config: Dict[str, Any], db_path: str = DB_PATH) -> int:
    now = time.time()
    with connect(db_path) as conn:
        cur = conn.execute(
            "INSERT INTO jobs(type, status, config_json, started_at, updated_at) "
            "VALUES(?,?,?,?,?)",
            (job_type, "queued", json.dumps(config), now, now),
        )
        return cur.lastrowid


def update_job(job_id: int, db_path: str = DB_PATH, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = time.time()
    sets = ",".join(f"{k}=?" for k in fields)
    with connect(db_path) as conn:
        conn.execute(f"UPDATE jobs SET {sets} WHERE id=?", (*fields.values(), job_id))


def get_job(job_id: int, db_path: str = DB_PATH) -> Optional[Dict[str, Any]]:
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    return dict(row) if row else None


def latest_job(job_type: Optional[str] = None, db_path: str = DB_PATH) -> Optional[Dict[str, Any]]:
    sql = "SELECT * FROM jobs"
    params: tuple = ()
    if job_type:
        sql += " WHERE type=?"
        params = (job_type,)
    sql += " ORDER BY id DESC LIMIT 1"
    with connect(db_path) as conn:
        row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def list_jobs(limit: int = 30, db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    return [dict(r) for r in rows]


def request_stop(job_id: int, db_path: str = DB_PATH) -> None:
    update_job(job_id, stop_requested=1)


# ---------------------------------------------------------------------------
# Staging → validate → promote (per-job governance)
# ---------------------------------------------------------------------------
# Master table -> primary-key field(s) used for dedup/promotion.
STAGE_PK = {
    "courses": ["course_id"],
    "colleges": ["college_id"],
    "offerings": ["course_id", "college_id"],
    "college_courses": ["college_id", "course_name"],
    "colleges_directory": ["college_id"],
}
_UPSERTERS = {
    "courses": "upsert_courses", "colleges": "upsert_colleges",
    "offerings": "upsert_offerings", "college_courses": "upsert_college_courses",
    "colleges_directory": "upsert_colleges_directory",
}


def stage_records(job_id: int, table_name: str, rows: Iterable[Dict[str, Any]],
                  db_path: str = DB_PATH) -> int:
    """Write a job's scraped rows to staging (not master). Deduped per job by PK."""
    pk_fields = STAGE_PK[table_name]
    rows = list(rows)
    payload = []
    for r in rows:
        if any(r.get(f) is None for f in pk_fields):
            continue
        pk = "|".join(str(r[f]) for f in pk_fields)
        payload.append((job_id, table_name, pk, json.dumps(r), time.time()))
    if not payload:
        return 0
    with connect(db_path) as conn:
        conn.executemany(
            "INSERT INTO staging(job_id, table_name, pk, payload, staged_at) "
            "VALUES(?,?,?,?,?) ON CONFLICT(job_id, table_name, pk) DO UPDATE SET "
            "payload=excluded.payload, staged_at=excluded.staged_at", payload)
    return len(payload)


def staged_summary(job_id: int, db_path: str = DB_PATH) -> Dict[str, int]:
    with connect(db_path) as conn:
        return {r[0]: r[1] for r in conn.execute(
            "SELECT table_name, COUNT(*) FROM staging WHERE job_id=? GROUP BY table_name",
            (job_id,)).fetchall()}


def count_promoted(job_id: int, table: str, db_path: str = DB_PATH) -> int:
    """Rows in a master table promoted by this job (source_job_id). Used with
    incremental promotion, where staging is emptied as data lands in master."""
    with connect(db_path) as conn:
        try:
            return conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE source_job_id=?", (job_id,)).fetchone()[0]
        except Exception:  # noqa: BLE001
            return 0


def get_staged_rows(job_id: int, table_name: str, limit: int = 1000,
                    db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT payload FROM staging WHERE job_id=? AND table_name=? LIMIT ?",
            (job_id, table_name, int(limit))).fetchall()
    return [json.loads(r[0]) for r in rows]


def diff_job(job_id: int, db_path: str = DB_PATH) -> Dict[str, Dict[str, int]]:
    """For each staged table, how many rows are new vs already in master."""
    out: Dict[str, Dict[str, int]] = {}
    with connect(db_path) as conn:
        for tbl, pkf in STAGE_PK.items():
            staged = conn.execute(
                "SELECT pk FROM staging WHERE job_id=? AND table_name=?",
                (job_id, tbl)).fetchall()
            if not staged:
                continue
            mcols = {r[1] for r in conn.execute(f"PRAGMA table_info({tbl})")}
            if not all(f in mcols for f in pkf):
                out[tbl] = {"staged": len(staged), "new": 0, "update": 0}
                continue
            existing = set()
            sel = "||'|'||".join(f"CAST({f} AS TEXT)" for f in pkf) if len(pkf) > 1 else f"CAST({pkf[0]} AS TEXT)"
            for r in conn.execute(f"SELECT {sel} FROM {tbl}"):
                existing.add(r[0])
            new = sum(1 for (pk,) in staged if pk not in existing)
            out[tbl] = {"staged": len(staged), "new": new, "update": len(staged) - new}
    return out


def validate_job(job_id: int, rules: Optional[Dict[str, Any]] = None,
                 db_path: str = DB_PATH) -> Dict[str, Any]:
    """Score a job's staged data 0-100 against simple rules. Streams the staging
    rows one at a time (O(1) memory — safe for very large jobs on small hosts).
    Returns {score, passed, checks:[...], total}."""
    rules = rules or {}
    min_rows = int(rules.get("min_rows", 1))
    max_missing_pct = float(rules.get("max_missing_fee_pct", 100))  # default lenient
    pass_score = float(rules.get("pass_score", 70))

    total = fee_rows = fee_missing = blanks = 0
    with connect(db_path) as conn:
        for r in conn.execute(
                "SELECT table_name, payload FROM staging WHERE job_id=?", (job_id,)):
            tbl = r[0]
            try:
                o = json.loads(r[1])
            except (json.JSONDecodeError, TypeError):
                o = {}
            total += 1
            if tbl in ("offerings", "college_courses"):
                fee_rows += 1
                if not (o.get("fees_amount") or o.get("total_fees")):
                    fee_missing += 1
            if tbl in ("courses", "offerings") and not o.get("name") and not o.get("course_name"):
                blanks += 1

    checks = [{"check": "rows staged", "value": total, "ok": total >= min_rows}]
    score = 100.0
    if total < min_rows:
        score -= 50
    if fee_rows:
        pct = 100.0 * fee_missing / max(1, fee_rows)
        ok = pct <= max_missing_pct
        checks.append({"check": "missing-fee %", "value": round(pct, 1), "ok": ok})
        if not ok:
            score -= min(40, pct - max_missing_pct)
    checks.append({"check": "blank-name rows", "value": blanks, "ok": blanks == 0})
    if blanks:
        score -= min(20, blanks)
    score = max(0.0, round(score, 1))
    passed = total >= min_rows and score >= pass_score
    return {"score": score, "passed": passed, "checks": checks, "total": total}


def flush_job_staging(job_id: int, db_path: str = DB_PATH,
                      chunk: int = PROMOTE_CHUNK) -> Dict[str, int]:
    """Promote a job's staged rows into master in MEMORY-SAFE CHUNKS, stamping
    source_job_id, and delete each promoted chunk from staging as it goes. Only
    `chunk` rows are held in RAM at once (PROMOTE_CHUNK, sized to the host), so it
    can be called repeatedly during a run (incremental promotion) — an interrupted
    job then loses at most the last un-flushed chunk. Returns {table: count}."""
    import sys as _sys
    me = _sys.modules[__name__]
    summary: Dict[str, int] = {}
    while True:
        with connect(db_path) as conn:
            batch = conn.execute(
                "SELECT id, table_name, payload FROM staging WHERE job_id=? "
                "ORDER BY id LIMIT ?", (job_id, int(chunk))).fetchall()
        if not batch:
            break
        by_table: Dict[str, List[Dict[str, Any]]] = {}
        ids: List[int] = []
        for r in batch:
            ids.append(r["id"])
            try:
                obj = json.loads(r["payload"])
            except (json.JSONDecodeError, TypeError):
                continue
            obj["source_job_id"] = job_id
            by_table.setdefault(r["table_name"], []).append(obj)
        for tbl, rows in by_table.items():
            getattr(me, _UPSERTERS[tbl])(rows, db_path=db_path)
            summary[tbl] = summary.get(tbl, 0) + len(rows)
        with connect(db_path) as conn:
            conn.execute(f"DELETE FROM staging WHERE id IN ({','.join('?' * len(ids))})", ids)
    return summary


def promote_job(job_id: int, db_path: str = DB_PATH) -> Dict[str, int]:
    """Merge a job's staged rows into master (chunked, memory-safe) and mark it
    promoted. Clears staged rows as they are promoted."""
    summary = flush_job_staging(job_id, db_path=db_path)
    update_job(job_id, promote_status="promoted", db_path=db_path)
    return summary


def flush_all_staging(db_path: str = DB_PATH, chunk: int = PROMOTE_CHUNK) -> Dict[str, int]:
    """Promote EVERY job's staged rows into master (chunked, memory-safe), clearing
    staging as it goes. Drains a backlog left by interrupted/crashed jobs. Upserts,
    so it dedupes against what's already in master. Returns {table: promoted}."""
    with connect(db_path) as conn:
        jids = [r[0] for r in conn.execute("SELECT DISTINCT job_id FROM staging")]
    total: Dict[str, int] = {}
    for jid in jids:
        summ = flush_job_staging(jid, db_path=db_path, chunk=chunk)
        for k, v in summ.items():
            total[k] = total.get(k, 0) + v
        try:
            update_job(jid, promote_status="promoted", db_path=db_path)
        except Exception:  # noqa: BLE001
            pass
    return total


def discard_all_staging(db_path: str = DB_PATH) -> int:
    with connect(db_path) as conn:
        return conn.execute("DELETE FROM staging").rowcount


def staging_count(db_path: str = DB_PATH) -> int:
    with connect(db_path) as conn:
        return conn.execute("SELECT COUNT(*) FROM staging").fetchone()[0]


def discard_staging(job_id: int, db_path: str = DB_PATH) -> int:
    with connect(db_path) as conn:
        cur = conn.execute("DELETE FROM staging WHERE job_id=?", (job_id,))
        return cur.rowcount


def wipe_data(keep_colleges: bool = True, db_path: str = DB_PATH) -> Dict[str, int]:
    """Reset to a clean slate, always keeping the Phase-1 `courses` table and
    saved settings/proxy config.

    keep_colleges=True  -> keep courses + colleges (clear Phase 2 offerings,
                           Phase 4 college_courses, all progress, staging, logs,
                           jobs, snapshots).
    keep_colleges=False -> keep ONLY courses (Phase 1); also clears the colleges
                           table, so Phases 2-4 start completely fresh.
    Returns rows deleted per table."""
    tables = ["offerings", "college_courses", "offering_progress",
              "cc_progress", "colleges_directory", "dir_progress",
              "staging", "logs", "jobs", "snapshots"]
    if not keep_colleges:
        tables.insert(0, "colleges")
    deleted: Dict[str, int] = {}
    with connect(db_path) as conn:
        for tbl in tables:
            try:
                cur = conn.execute(f"DELETE FROM {tbl}")
                deleted[tbl] = cur.rowcount
            except Exception:
                deleted[tbl] = 0
        # drop the phase-1 resume pointer so the next run starts fresh
        conn.execute("DELETE FROM settings WHERE key='courses_resume_page'")
    return deleted


def wipe_except_courses_colleges(db_path: str = DB_PATH) -> Dict[str, int]:
    """Back-compat wrapper: keep courses + colleges, clear the rest."""
    return wipe_data(keep_colleges=True, db_path=db_path)


# ---------------------------------------------------------------------------
# Live logs (persisted)
# ---------------------------------------------------------------------------
def add_log(job_id: int, message: str, db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute("INSERT INTO logs(job_id, ts, message) VALUES(?,?,?)",
                     (job_id, time.time(), (message or "")[:600]))


def get_logs(job_id: Optional[int] = None, limit: int = 200, after_id: int = 0,
             db_path: str = DB_PATH) -> List[Dict[str, Any]]:
    with connect(db_path) as conn:
        if job_id:
            rows = conn.execute(
                "SELECT id, ts, message FROM logs WHERE job_id=? AND id>? "
                "ORDER BY id DESC LIMIT ?", (job_id, after_id, limit)).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, ts, job_id, message FROM logs WHERE id>? "
                "ORDER BY id DESC LIMIT ?", (after_id, limit)).fetchall()
    return [dict(r) for r in rows]


def clear_logs(job_id: Optional[int] = None, db_path: str = DB_PATH) -> int:
    with connect(db_path) as conn:
        if job_id:
            cur = conn.execute("DELETE FROM logs WHERE job_id=?", (job_id,))
        else:
            cur = conn.execute("DELETE FROM logs")
        return cur.rowcount


def prune_logs(keep: int = 8000, db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "DELETE FROM logs WHERE id NOT IN "
            "(SELECT id FROM logs ORDER BY id DESC LIMIT ?)", (keep,))


def mark_stale_jobs_interrupted(idle_sec: int = 300, db_path: str = DB_PATH) -> int:
    """A 'running' job whose updated_at is older than idle_sec means its worker
    died (container restart, crash). Flag it so the UI can offer a resume."""
    cutoff = time.time() - idle_sec
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id FROM jobs WHERE status IN ('running','queued') AND updated_at < ?",
            (cutoff,),
        ).fetchall()
        for r in rows:
            conn.execute(
                "UPDATE jobs SET status='stopped', "
                "message='interrupted (worker died) — resume to continue', "
                "finished_at=? WHERE id=?",
                (time.time(), r["id"]),
            )
    return len(rows)


def resume_job(job_id: int, db_path: str = DB_PATH) -> None:
    """Re-queue an existing job so a worker picks it up and continues from its
    saved progress (offering_progress / courses_resume_page / dedupe). Clearing
    promote_status lets it auto-promote the full staged set when it completes —
    even if some partial data was already promoted manually."""
    update_job(job_id, status="queued", stop_requested=0, finished_at=None,
               promote_status=None, db_path=db_path)


def stop_requested(job_id: int, db_path: str = DB_PATH) -> bool:
    with connect(db_path) as conn:
        row = conn.execute("SELECT stop_requested FROM jobs WHERE id=?", (job_id,)).fetchone()
    return bool(row and row["stop_requested"])


# ---------------------------------------------------------------------------
# Counts for the dashboard
# ---------------------------------------------------------------------------
def counts(db_path: str = DB_PATH) -> Dict[str, int]:
    with connect(db_path) as conn:
        def one(q: str) -> int:
            return conn.execute(q).fetchone()[0]
        return {
            "courses": one("SELECT COUNT(*) FROM courses"),
            "colleges": one("SELECT COUNT(*) FROM colleges"),
            "offerings": one("SELECT COUNT(*) FROM offerings"),
            "courses_done_phase2": one("SELECT COUNT(*) FROM offering_progress WHERE status='done'"),
        }


if __name__ == "__main__":
    init_db()
    print(f"Initialized DB at {DB_PATH}")
    print(counts())
