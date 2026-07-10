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
                         ("application_end", "TEXT")):
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
        for tbl in ("courses", "colleges", "offerings", "college_courses"):
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
def upsert_courses(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
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
    placeholders = ",".join("?" for _ in cols)
    sql = (
        f"INSERT INTO courses ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(course_id) DO UPDATE SET "
        + ",".join(f"{c}=excluded.{c}" for c in cols if c != "course_id")
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
    cols = ["website", "email", "phone", "rating_value", "rating_count",
            "pros", "cons", "address"]
    sets = ", ".join(f"{c}=?" for c in cols) + ", enriched_at=?"
    vals = [fields.get(c) for c in cols] + [time.time(), college_id]
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


def upsert_college_courses(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
    rows = [r for r in rows if r.get("course_name")]
    if not rows:
        return 0
    cols = ["college_id", "college_name", "course_name", "specialization",
            "eligibility", "total_fees", "fees_inr", "hostel_fees", "duration",
            "mode", "level", "course_type", "rating", "reviews_count",
            "application_start", "application_end", "source_url", "scraped_at",
            "source_job_id"]
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


def list_known_college_ids(db_path: str = DB_PATH) -> List[int]:
    with connect(db_path) as conn:
        return [r[0] for r in conn.execute(
            "SELECT college_id FROM colleges ORDER BY college_id")]


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
}
_UPSERTERS = {
    "courses": "upsert_courses", "colleges": "upsert_colleges",
    "offerings": "upsert_offerings", "college_courses": "upsert_college_courses",
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
    """Score a job's staged data 0-100 against simple rules. Returns
    {score, passed, checks:[...], total}."""
    rules = rules or {}
    min_rows = int(rules.get("min_rows", 1))
    max_missing_pct = float(rules.get("max_missing_fee_pct", 100))  # default lenient
    pass_score = float(rules.get("pass_score", 70))

    rows_by_table: Dict[str, List[Dict[str, Any]]] = {}
    with connect(db_path) as conn:
        for r in conn.execute(
                "SELECT table_name, payload FROM staging WHERE job_id=?", (job_id,)):
            rows_by_table.setdefault(r[0], []).append(json.loads(r[1]))

    total = sum(len(v) for v in rows_by_table.values())
    checks = []
    score = 100.0

    checks.append({"check": "rows staged", "value": total,
                   "ok": total >= min_rows})
    if total < min_rows:
        score -= 50

    # fee completeness on the fee-bearing tables
    fee_rows = (rows_by_table.get("offerings", []) or
                rows_by_table.get("college_courses", []))
    if fee_rows:
        def has_fee(r):
            return bool(r.get("fees_amount") or r.get("total_fees"))
        missing = sum(1 for r in fee_rows if not has_fee(r))
        pct = 100.0 * missing / max(1, len(fee_rows))
        ok = pct <= max_missing_pct
        checks.append({"check": "missing-fee %", "value": round(pct, 1), "ok": ok})
        if not ok:
            score -= min(40, pct - max_missing_pct)

    # blank key fields
    blanks = 0
    for tbl, rws in rows_by_table.items():
        for r in rws:
            if tbl in ("courses", "offerings") and not r.get("name") and not r.get("course_name"):
                blanks += 1
    checks.append({"check": "blank-name rows", "value": blanks, "ok": blanks == 0})
    if blanks:
        score -= min(20, blanks)

    score = max(0.0, round(score, 1))
    passed = total >= min_rows and score >= pass_score
    return {"score": score, "passed": passed, "checks": checks, "total": total}


def promote_job(job_id: int, db_path: str = DB_PATH) -> Dict[str, int]:
    """Merge a job's staged rows into master, stamping source_job_id."""
    import sys as _sys
    me = _sys.modules[__name__]
    by_table: Dict[str, List[Dict[str, Any]]] = {}
    with connect(db_path) as conn:
        for r in conn.execute(
                "SELECT table_name, payload FROM staging WHERE job_id=?", (job_id,)):
            by_table.setdefault(r[0], []).append(json.loads(r[1]))
    summary: Dict[str, int] = {}
    for tbl, rows in by_table.items():
        for row in rows:
            row["source_job_id"] = job_id
        getattr(me, _UPSERTERS[tbl])(rows, db_path=db_path)
        summary[tbl] = len(rows)
    update_job(job_id, promote_status="promoted", db_path=db_path)
    return summary


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
              "cc_progress", "staging", "logs", "jobs", "snapshots"]
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
