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
"""


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
            "colleges_done": one("SELECT COUNT(*) FROM sk_college_progress "
                                 "WHERE status IN ('done','gone')"),
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
