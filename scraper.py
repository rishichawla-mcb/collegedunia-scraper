"""
Scraper engine for collegedunia.com/course-finder.

Everything funnels through one JSON endpoint:

    https://collegedunia.com/web-api/listing-cf?data=<base64-json>

  * {"page": N}                -> a page of COURSES (phase 1)
  * {"page": N, "course_id": X} -> a page of COLLEGES offering course X (phase 2)

This module provides:

  * ProxyManager  - rotation over a pasted list and/or a provider gateway, with
                    health tracking so dead proxies get skipped.
  * Client        - performs a single API request with retries, proxy rotation,
                    and block detection (403/429/empty/HTML-instead-of-JSON).
  * run_courses() - phase 1: walk all course pages into the DB.
  * run_offerings() - phase 2: for each course, walk all college pages into the DB.

Both runners checkpoint to the DB and honour a cooperative stop flag, so a run
can be interrupted and resumed.

NOTE: This rotates proxies you supply and backs off politely. It intentionally
does NOT solve CAPTCHAs or spoof browser fingerprints.
"""

from __future__ import annotations

import base64
import itertools
import json
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import requests

import db

API_URL = "https://collegedunia.com/web-api/listing-cf"
SITE = "https://collegedunia.com"
PAGE_SIZE = 10

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


def base_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"{SITE}/course-finder",
        "X-Requested-With": "XMLHttpRequest",
    }


def encode_payload(payload: Dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def abs_url(path: Optional[str]) -> str:
    if not path:
        return ""
    if path.startswith("http"):
        return path
    return f"{SITE}/{path.lstrip('/')}"


class BlockedError(Exception):
    """Raised when the site appears to be blocking us (403/429/HTML challenge)."""


# ---------------------------------------------------------------------------
# Proxy management
# ---------------------------------------------------------------------------
@dataclass
class Proxy:
    url: str
    fails: int = 0
    cooldown_until: float = 0.0

    def as_dict(self) -> Dict[str, str]:
        return {"http": self.url, "https": self.url}


@dataclass
class ProxyManager:
    """
    mode:
      "none"    -> direct connection (your own IP)
      "list"    -> rotate over `proxies`, skipping ones in cooldown
      "gateway" -> always use `gateway` (a provider endpoint that rotates IPs
                   server-side, e.g. http://user:pass@gateway.provider.com:7777)
    """
    mode: str = "none"
    proxies: List[Proxy] = field(default_factory=list)
    gateway: Optional[str] = None
    cooldown_seconds: float = 120.0
    max_fails: int = 3
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _cycle: Any = None

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "ProxyManager":
        mode = cfg.get("proxy_mode", "none")
        proxies = [Proxy(url=u.strip()) for u in cfg.get("proxy_list", []) if u.strip()]
        return cls(
            mode=mode,
            proxies=proxies,
            gateway=(cfg.get("proxy_gateway") or None),
            cooldown_seconds=float(cfg.get("proxy_cooldown", 120)),
        )

    def __post_init__(self) -> None:
        self._cycle = itertools.cycle(self.proxies) if self.proxies else None

    def get(self) -> Optional[Proxy]:
        if self.mode == "none":
            return None
        if self.mode == "gateway":
            return Proxy(url=self.gateway) if self.gateway else None
        # list mode
        with self._lock:
            if not self.proxies:
                return None
            now = time.time()
            for _ in range(len(self.proxies)):
                p = next(self._cycle)
                if p.cooldown_until <= now:
                    return p
            # all cooling down -> return the soonest-available anyway
            return min(self.proxies, key=lambda p: p.cooldown_until)

    def report_failure(self, proxy: Optional[Proxy]) -> None:
        if proxy is None or self.mode != "list":
            return
        with self._lock:
            proxy.fails += 1
            proxy.cooldown_until = time.time() + self.cooldown_seconds * proxy.fails

    def report_success(self, proxy: Optional[Proxy]) -> None:
        if proxy is None or self.mode != "list":
            return
        with self._lock:
            proxy.fails = 0
            proxy.cooldown_until = 0.0

    def healthy_count(self) -> int:
        if self.mode != "list":
            return 1 if (self.mode == "gateway" and self.gateway) else 0
        now = time.time()
        return sum(1 for p in self.proxies if p.cooldown_until <= now)


def test_proxy(url: str, timeout: float = 15.0) -> Dict[str, Any]:
    """Quick health check used by the UI 'test proxies' button."""
    proxies = {"http": url, "https": url}
    started = time.time()
    try:
        r = requests.get("https://api.ipify.org?format=json", proxies=proxies,
                         timeout=timeout, headers={"User-Agent": USER_AGENTS[0]})
        r.raise_for_status()
        return {"url": url, "ok": True, "ip": r.json().get("ip"),
                "ms": int((time.time() - started) * 1000)}
    except Exception as err:  # noqa: BLE001
        return {"url": url, "ok": False, "error": str(err)[:120]}


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------
class Client:
    def __init__(self, proxy_manager: ProxyManager, timeout: float = 30.0,
                 max_retries: int = 5, backoff: float = 4.0,
                 log: Optional[Callable[[str], None]] = None) -> None:
        self.pm = proxy_manager
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.session = requests.Session()
        self.log = log or (lambda m: None)

    def fetch(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        params = {"data": encode_payload(payload)}
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            proxy = self.pm.get()
            try:
                resp = self.session.get(
                    API_URL, params=params, headers=base_headers(),
                    proxies=proxy.as_dict() if proxy else None, timeout=self.timeout,
                )
                if resp.status_code in (403, 429, 503):
                    raise BlockedError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "")
                if "json" not in ctype and not resp.text.lstrip().startswith("{"):
                    raise BlockedError("non-JSON response (challenge page?)")
                data = resp.json()
                self.pm.report_success(proxy)
                return data
            except (BlockedError, requests.RequestException, ValueError) as err:
                last_err = err
                self.pm.report_failure(proxy)
                wait = self.backoff * attempt + random.uniform(0, 2)
                via = proxy.url if proxy else "direct"
                self.log(f"  ! attempt {attempt}/{self.max_retries} via {via} failed: "
                         f"{err} -> retry in {wait:.0f}s")
                time.sleep(wait)
        raise RuntimeError(f"Failed after {self.max_retries} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Record flattening
# ---------------------------------------------------------------------------
def parse_course(c: Dict[str, Any]) -> Dict[str, Any]:
    lp = c.get("lead_params") or {}
    exam = c.get("exam") or {}
    cd = c.get("colleges_data") or {}
    return {
        "course_id": _to_int(lp.get("course_id")),
        "name": c.get("name", ""),
        "course_link": abs_url(c.get("course_link")),
        "listing_link": abs_url(c.get("link")),
        "duration": c.get("duration", ""),
        "course_type": c.get("course_type", ""),
        "level": c.get("level", ""),
        "eligibility": c.get("eligibility", ""),
        "program_type": c.get("courses_could_be", ""),
        "mode": c.get("degree_could_be", ""),
        "exam_name": exam.get("name", ""),
        "exam_url": abs_url(exam.get("url")),
        "fees": c.get("fees", ""),
        "avg_salary": c.get("avg_salary", ""),
        "colleges_count": _to_int(cd.get("count")),
        "job_roles": ", ".join(c.get("job_roles") or []),
        "topics_covered": ", ".join(c.get("topics_covered") or []),
        "stream_id": str(lp.get("stream_id", "")),
        "course_tag": lp.get("course_tag", ""),
        "course_tag_id": str(lp.get("course_tag_id", "")),
        "description": c.get("description", ""),
        "colleges_url": abs_url(cd.get("link")),
        "raw_json": json.dumps(c, ensure_ascii=False),
        "scraped_at": time.time(),
    }


def parse_offering(course_id: int, c: Dict[str, Any]) -> Dict[str, Any]:
    col = c.get("college") or {}
    exam = c.get("exam") or {}
    fees = c.get("fees_data") or {}
    rank = c.get("ranking_data") or {}
    if isinstance(rank, list):
        rank = {}
    cutoff = c.get("cutoff") or {}
    if isinstance(cutoff, list):
        cutoff = {}
    adm = c.get("admission") or {}
    lp = c.get("lead_params") or {}
    return {
        "course_id": course_id,
        "college_id": _to_int(lp.get("college_id") or col.get("college_id")),
        "course_name": c.get("name", ""),
        "course_acronym": lp.get("course_acronym", ""),
        "college_name": col.get("name", ""),
        "college_short": col.get("short_form", ""),
        "city": col.get("city", ""),
        "state_id": _to_int(col.get("state_id")),
        "logo": col.get("logo", ""),
        "fees_amount": _to_int(fees.get("amount")),
        "fees_text": fees.get("text", ""),
        "eligibility": c.get("eligibility", ""),
        "exam_name": exam.get("name", ""),
        "exam_url": abs_url(exam.get("url")),
        "duration": c.get("duration", ""),
        "course_type": c.get("course_type", ""),
        "level": c.get("level", ""),
        "program_type": c.get("course_could_be", ""),
        "mode": c.get("degree_could_be", ""),
        "ranking_rank": _to_int(rank.get("rank")),
        "ranking_agency": rank.get("agency", ""),
        "ranking_total": _to_int(rank.get("total")),
        "ranking_stream": rank.get("stream", ""),
        "ranking_url": abs_url(rank.get("url")),
        "course_rating": _to_float(c.get("course_rating")),
        "reviews_count": _to_int(c.get("reviews_count")),
        "major_stream_rating": _to_float(lp.get("major_stream_rating")),
        "cutoff_exam": cutoff.get("exam_name", ""),
        "cutoff_value": _to_int(cutoff.get("cutoff")),
        "admission_start": adm.get("admission_start_date", ""),
        "admission_end": adm.get("admission_end_date", ""),
        "job_roles": ", ".join(c.get("job_roles") or []),
        "topics_covered": ", ".join(c.get("topics_covered") or []),
        "description": c.get("description", ""),
        "stream_id": str(lp.get("stream_id", "")),
        "course_tag": lp.get("course_tag", ""),
        "course_tag_id": str(lp.get("course_tag_id", "")),
        "university_link": abs_url(c.get("link")),
        "raw_json": json.dumps(c, ensure_ascii=False),
        "scraped_at": time.time(),
    }


def parse_college(c: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    col = c.get("college") or {}
    lp = c.get("lead_params") or {}
    cid = _to_int(lp.get("college_id") or col.get("college_id"))
    if cid is None:
        return None
    return {
        "college_id": cid,
        "name": col.get("name", ""),
        "short_form": col.get("short_form", ""),
        "city": col.get("city", ""),
        "state_id": _to_int(col.get("state_id")),
        "link": abs_url(col.get("link")),
        "logo": col.get("logo", ""),
        "raw_json": json.dumps(col, ensure_ascii=False),
        "scraped_at": time.time(),
    }


def _to_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Phase 1: courses
# ---------------------------------------------------------------------------
def run_courses(job_id: int, cfg: Dict[str, Any], db_path: str = db.DB_PATH,
                log: Optional[Callable[[str], None]] = None) -> None:
    log = log or (lambda m: print(m, flush=True))
    pm = ProxyManager.from_config(cfg)
    client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                    backoff=float(cfg.get("backoff", 4)))
    delay = float(cfg.get("delay", 1.0))
    max_pages = cfg.get("max_pages")
    start_page = int(cfg.get("start_page", 1))

    db.update_job(job_id, status="running", message="starting courses scrape",
                  db_path=db_path)
    page = start_page
    pages_done = 0
    written = 0
    total = None
    try:
        while True:
            if db.stop_requested(job_id, db_path=db_path):
                db.update_job(job_id, status="stopped", message="stopped by user",
                              finished_at=time.time(), db_path=db_path)
                log("Stopped by user.")
                return
            data = client.fetch({"page": page})
            courses = data.get("courses") or []
            if total is None:
                total = data.get("count")
                db.update_job(job_id, total_units=(total or 0), db_path=db_path)
            if not courses:
                break
            parsed = [parse_course(c) for c in courses if (c.get("lead_params") or {}).get("course_id")]
            written += db.upsert_courses(parsed, db_path=db_path)
            pages_done += 1
            db.update_job(job_id, done_units=written, items_written=written,
                          message=f"page {page}: {written} courses", db_path=db_path)
            log(f"  page {page}: +{len(parsed)} (total {written})")
            page += 1
            if max_pages and pages_done >= int(max_pages):
                break
            if not data.get("hasNext", False):
                break
            time.sleep(delay)
        db.update_job(job_id, status="completed", message=f"done: {written} courses",
                      finished_at=time.time(), db_path=db_path)
        log(f"Done. {written} courses.")
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise


# ---------------------------------------------------------------------------
# Phase 2: colleges offering each course
# ---------------------------------------------------------------------------
def run_offerings(job_id: int, cfg: Dict[str, Any], db_path: str = db.DB_PATH,
                  log: Optional[Callable[[str], None]] = None) -> None:
    log = log or (lambda m: print(m, flush=True))
    pm = ProxyManager.from_config(cfg)
    client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                    backoff=float(cfg.get("backoff", 4)))
    delay = float(cfg.get("delay", 1.0))
    max_pages_per_course = cfg.get("max_pages_per_course")

    # Which courses to process?
    course_ids: List[int] = cfg.get("course_ids") or []
    if not course_ids:
        where = cfg.get("course_where", "")
        params = tuple(cfg.get("course_where_params", []))
        course_ids = db.list_course_ids(db_path=db_path, where=where, params=params)

    if not cfg.get("force_rescrape"):
        done = db.get_done_course_ids(db_path=db_path)
        course_ids = [cid for cid in course_ids if cid not in done]

    db.update_job(job_id, status="running", total_units=len(course_ids),
                  message=f"{len(course_ids)} courses to process", db_path=db_path)
    log(f"Phase 2: {len(course_ids)} courses to process.")

    processed = 0
    offerings_written = 0
    try:
        for cid in course_ids:
            if db.stop_requested(job_id, db_path=db_path):
                db.update_job(job_id, status="stopped",
                              message=f"stopped after {processed} courses",
                              finished_at=time.time(), db_path=db_path)
                log("Stopped by user.")
                return
            page = 1
            course_total = None
            while True:
                if db.stop_requested(job_id, db_path=db_path):
                    break
                data = client.fetch({"page": page, "course_id": cid})
                rows = data.get("courses") or []
                if course_total is None:
                    course_total = data.get("count")
                if not rows:
                    break
                colleges = [pc for pc in (parse_college(r) for r in rows) if pc]
                offerings = [parse_offering(cid, r) for r in rows]
                db.upsert_colleges(colleges, db_path=db_path)
                offerings_written += db.upsert_offerings(offerings, db_path=db_path)
                db.set_offering_progress(cid, "partial", page, course_total, db_path=db_path)
                page += 1
                if max_pages_per_course and page > int(max_pages_per_course):
                    break
                if not data.get("hasNext", False):
                    break
                time.sleep(delay)
            db.set_offering_progress(cid, "done", page - 1, course_total, db_path=db_path)
            processed += 1
            db.update_job(job_id, done_units=processed, items_written=offerings_written,
                          message=f"{processed}/{len(course_ids)} courses, "
                                  f"{offerings_written} offerings", db_path=db_path)
            log(f"  course {cid}: ~{course_total} colleges "
                f"({processed}/{len(course_ids)} done)")
        db.update_job(job_id, status="completed",
                      message=f"done: {processed} courses, {offerings_written} offerings",
                      finished_at=time.time(), db_path=db_path)
        log(f"Done. {processed} courses, {offerings_written} offerings.")
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise
