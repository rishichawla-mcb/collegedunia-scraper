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
import sqlite3
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional

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

CREATE INDEX IF NOT EXISTS idx_offerings_course ON offerings(course_id);
CREATE INDEX IF NOT EXISTS idx_offerings_college ON offerings(college_id);
CREATE INDEX IF NOT EXISTS idx_cc_college ON college_courses(college_id);
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
        "colleges_url", "raw_json", "scraped_at",
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
            "raw_json", "scraped_at"]
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


def upsert_college_courses(rows: Iterable[Dict[str, Any]], db_path: str = DB_PATH) -> int:
    rows = [r for r in rows if r.get("course_name")]
    if not rows:
        return 0
    cols = ["college_id", "college_name", "course_name", "eligibility",
            "total_fees", "hostel_fees", "source_url", "scraped_at"]
    ph = ",".join("?" for _ in cols)
    sql = (f"INSERT INTO college_courses ({','.join(cols)}) VALUES ({ph}) "
           f"ON CONFLICT(college_id, course_name) DO UPDATE SET "
           + ",".join(f"{c}=excluded.{c}" for c in cols
                      if c not in ("college_id", "course_name")))
    with connect(db_path) as conn:
        conn.executemany(sql, [tuple(r.get(c) for c in cols) for r in rows])
    return len(rows)


def set_cc_progress(college_id: int, status: str, found: int, db_path: str = DB_PATH) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "INSERT INTO cc_progress(college_id, status, found, updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(college_id) DO UPDATE SET status=excluded.status, "
            "found=excluded.found, updated_at=excluded.updated_at",
            (college_id, status, found, time.time()))


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
    saved progress (offering_progress / courses_resume_page / dedupe)."""
    update_job(job_id, status="queued", stop_requested=0, finished_at=None, db_path=db_path)


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
