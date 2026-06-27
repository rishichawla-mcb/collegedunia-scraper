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


class BudgetExceeded(Exception):
    """Raised when a configured request/bandwidth budget is hit."""


class Stats:
    """Thread-safe counters shared across concurrent workers."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.requests = 0
        self.bytes = 0
        self.blocks = 0

    def add(self, requests: int = 0, byts: int = 0, blocks: int = 0) -> None:
        with self._lock:
            self.requests += requests
            self.bytes += byts
            self.blocks += blocks

    def snapshot(self):
        with self._lock:
            return self.requests, self.bytes, self.blocks


class AdaptiveDelay:
    """Auto-tunes the inter-request delay: slows down when blocks happen,
    eases back toward the base delay during clean stretches."""

    def __init__(self, base: float, enabled: bool = True,
                 max_delay: float = 30.0) -> None:
        self.base = base
        self.current = base
        self.enabled = enabled
        self.max_delay = max_delay
        self._clean = 0
        self._lock = threading.Lock()

    def on_block(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            self.current = min(self.max_delay, max(self.current * 1.8, self.base * 2))
            self._clean = 0

    def on_success(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._clean += 1
            if self._clean >= 20 and self.current > self.base:
                self.current = max(self.base, self.current * 0.8)
                self._clean = 0

    def value(self) -> float:
        with self._lock:
            return self.current


def send_notification(cfg: Dict[str, Any], subject: str, body: str,
                      log: Optional[Callable[[str], None]] = None) -> None:
    """Best-effort notification via webhook and/or email (SMTP). Never raises."""
    log = log or (lambda m: None)
    # Webhook (e.g. Slack/Discord/generic): POST {"text": ...}
    hook = cfg.get("webhook_url")
    if hook:
        try:
            requests.post(hook, json={"text": f"*{subject}*\n{body}"}, timeout=15)
        except Exception as err:  # noqa: BLE001
            log(f"webhook notify failed: {err}")
    # Email via SMTP
    smtp = cfg.get("smtp") or {}
    if smtp.get("host") and smtp.get("to"):
        try:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = smtp.get("from", smtp.get("user", "scraper@local"))
            msg["To"] = smtp["to"]
            server = smtplib.SMTP(smtp["host"], int(smtp.get("port", 587)), timeout=20)
            if smtp.get("starttls", True):
                server.starttls()
            if smtp.get("user"):
                server.login(smtp["user"], smtp.get("password", ""))
            server.sendmail(msg["From"], [smtp["to"]], msg.as_string())
            server.quit()
        except Exception as err:  # noqa: BLE001
            log(f"email notify failed: {err}")


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
                 log: Optional[Callable[[str], None]] = None,
                 stats: Optional[Stats] = None,
                 adaptive: Optional[AdaptiveDelay] = None) -> None:
        self.pm = proxy_manager
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.session = requests.Session()
        self.log = log or (lambda m: None)
        self.stats = stats or Stats()
        self.adaptive = adaptive

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
                self.stats.add(requests=1, byts=len(resp.content or b""))
                if resp.status_code in (403, 429, 503):
                    raise BlockedError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "")
                if "json" not in ctype and not resp.text.lstrip().startswith("{"):
                    raise BlockedError("non-JSON response (challenge page?)")
                data = resp.json()
                self.pm.report_success(proxy)
                if self.adaptive:
                    self.adaptive.on_success()
                return data
            except (BlockedError, requests.RequestException, ValueError) as err:
                last_err = err
                self.stats.add(blocks=1)
                self.pm.report_failure(proxy)
                if self.adaptive:
                    self.adaptive.on_block()
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
        "stream_name": db.stream_name(lp.get("stream_id")),
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
# Facets used to partition the course list under the API's ~1,700-result
# pagination ceiling. Order matters: broad → narrow.
COURSE_TYPE_VALUES = ["Degree", "Diploma", "Certification"]
LEVEL_VALUES = ["Graduation", "Post Graduation", "10+2", "Doctorate/M.Phil", "10th"]
PARTITION_FACETS = [
    ("stream", list(range(1, 21))),          # MUST be ints — string values are ignored
    ("course_type", COURSE_TYPE_VALUES),
    ("level", LEVEL_VALUES),
]
PAGINATION_CAP = 1500  # stay safely under the ~1,700 ceiling


def run_courses(job_id: int, cfg: Dict[str, Any], db_path: str = db.DB_PATH,
                log: Optional[Callable[[str], None]] = None) -> None:
    log = log or (lambda m: print(m, flush=True))
    if cfg.get("partition"):
        return _run_courses_partitioned(job_id, cfg, db_path, log)
    pm = ProxyManager.from_config(cfg)
    client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                    backoff=float(cfg.get("backoff", 4)))
    delay = float(cfg.get("delay", 1.0))
    max_pages = cfg.get("max_pages")
    start_page = int(cfg.get("start_page", 1))

    # Resume: if not forced and no explicit start_page, continue from saved point.
    if start_page == 1 and not cfg.get("force_restart"):
        saved = db.get_setting("courses_resume_page", 1, db_path=db_path)
        try:
            start_page = max(1, int(saved))
        except (TypeError, ValueError):
            start_page = 1

    soft_retries = int(cfg.get("soft_block_retries", 4))

    db.update_job(job_id, status="running",
                  message=f"starting courses scrape at page {start_page}", db_path=db_path)
    page = start_page
    pages_done = 0
    written = 0
    total = None
    try:
        # Seed the running total from what's already in the DB (for resume).
        written = db.counts(db_path=db_path).get("courses", 0)
        while True:
            if db.stop_requested(job_id, db_path=db_path):
                db.set_setting("courses_resume_page", page, db_path=db_path)
                db.update_job(job_id, status="stopped",
                              message=f"stopped by user at page {page} ({written} courses)",
                              finished_at=time.time(), db_path=db_path)
                log("Stopped by user.")
                return

            data = client.fetch({"page": page})
            courses = data.get("courses") or []
            if total is None:
                total = data.get("count")
                db.update_job(job_id, total_units=(total or 0), db_path=db_path)

            # An empty page or hasNext=false BEFORE we've reached the known total
            # is almost always a soft rate-limit, not a real end. Treat it as a
            # block: retry the same page a few times, then stop INCOMPLETE so the
            # run can be resumed — never silently report "completed".
            reached_end = (total is not None and written >= (total - PAGE_SIZE))
            looks_blocked = (not courses or not data.get("hasNext", False)) and not reached_end \
                and not (max_pages and pages_done + 1 >= int(max_pages))

            if not courses and looks_blocked:
                ok = False
                for attempt in range(1, soft_retries + 1):
                    wait = 10 * attempt
                    log(f"  ! page {page} returned empty but only {written}/{total} done "
                        f"— likely a soft block. Retry {attempt}/{soft_retries} in {wait}s")
                    time.sleep(wait)
                    data = client.fetch({"page": page})
                    courses = data.get("courses") or []
                    if courses:
                        ok = True
                        break
                if not ok:
                    db.set_setting("courses_resume_page", page, db_path=db_path)
                    msg = (f"INCOMPLETE — blocked at page {page} ({written}/{total} courses). "
                           f"Increase delay / add proxies, then resume.")
                    db.update_job(job_id, status="stopped", message=msg,
                                  finished_at=time.time(), db_path=db_path)
                    log(msg)
                    return

            if not courses:
                break  # genuine end (we're at/near the known total)

            parsed = [parse_course(c) for c in courses if (c.get("lead_params") or {}).get("course_id")]
            db.upsert_courses(parsed, db_path=db_path)
            written = db.counts(db_path=db_path).get("courses", 0)
            pages_done += 1
            page += 1
            db.set_setting("courses_resume_page", page, db_path=db_path)
            db.update_job(job_id, done_units=written, items_written=written,
                          message=f"page {page-1}: {written}/{total} courses", db_path=db_path)
            log(f"  page {page-1}: +{len(parsed)} (total {written}/{total})")

            if max_pages and pages_done >= int(max_pages):
                break
            if not data.get("hasNext", False) and reached_end:
                break
            time.sleep(delay)

        db.set_setting("courses_resume_page", 1, db_path=db_path)  # reset for next full run
        db.update_job(job_id, status="completed",
                      message=f"done: {written} courses", finished_at=time.time(), db_path=db_path)
        log(f"Done. {written} courses.")
    except Exception as err:  # noqa: BLE001
        db.set_setting("courses_resume_page", page, db_path=db_path)
        db.update_job(job_id, status="error",
                      message=f"{str(err)[:240]} (resume from page {page})",
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise


def _run_courses_partitioned(job_id: int, cfg: Dict[str, Any], db_path: str,
                             log: Callable[[str], None]) -> None:
    """Complete Phase-1 scrape that defeats the API's ~1,700-result pagination
    ceiling by recursively splitting the query (stream -> type -> level) until
    each chunk is small enough to page through fully. course_id is the primary
    key, so overlaps dedupe automatically."""
    pm = ProxyManager.from_config(cfg)
    stats = Stats()
    adaptive = AdaptiveDelay(float(cfg.get("delay", 1.0)), enabled=bool(cfg.get("adaptive", True)))
    client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                    backoff=float(cfg.get("backoff", 4)), stats=stats, adaptive=adaptive)
    soft_retries = int(cfg.get("soft_block_retries", 4))
    long_cooldown = float(cfg.get("long_cooldown_seconds", 0))

    grand_total = {"v": None}
    leaves_done = {"v": 0}

    def stopped() -> bool:
        return db.stop_requested(job_id, db_path=db_path)

    def fetch_page(filters: Dict[str, Any], page: int) -> Dict[str, Any]:
        return client.fetch({"page": page, **filters})

    def push() -> None:
        reqs, byts, blocks = stats.snapshot()
        written = db.counts(db_path=db_path).get("courses", 0)
        db.update_job(job_id, done_units=written, items_written=written,
                      total_units=(grand_total["v"] or 0), req_count=reqs, bytes_count=byts,
                      message=f"{written}/{grand_total['v']} courses · {leaves_done['v']} chunks · "
                              f"{byts/1048576:.1f} MB", db_path=db_path)

    def page_through(filters: Dict[str, Any], first: Optional[Dict[str, Any]]) -> None:
        page = 1
        data = first
        while not stopped():
            if data is None:
                data = fetch_page(filters, page)
            rows = data.get("courses") or []
            if not rows:
                break
            parsed = [parse_course(c) for c in rows
                      if (c.get("lead_params") or {}).get("course_id")]
            db.upsert_courses(parsed, db_path=db_path)
            if not data.get("hasNext", False):
                break
            page += 1
            data = None
            if page % 5 == 0:
                push()
            time.sleep(adaptive.value())
        leaves_done["v"] += 1
        push()

    def recurse(filters: Dict[str, Any], idx: int, label: str) -> None:
        if stopped():
            return
        # probe the size of this slice
        data = None
        for attempt in range(1, soft_retries + 1):
            data = fetch_page(filters, 1)
            if (data.get("courses") or []) or data.get("count", 0) == 0:
                break
            wait = long_cooldown if long_cooldown else 8 * attempt
            log(f"  ! probe '{label}' empty — retry {attempt}/{soft_retries} in {wait:.0f}s")
            time.sleep(wait)
        count = data.get("count") or 0
        if grand_total["v"] is None:
            grand_total["v"] = count  # root count = full universe
        if count == 0:
            return
        if count <= PAGINATION_CAP or idx >= len(PARTITION_FACETS):
            if count > PAGINATION_CAP:
                log(f"  ~ '{label}' has {count} (> cap) and no finer facet — "
                    f"paging as deep as the API allows.")
            log(f"  → scraping chunk '{label}' (~{count})")
            page_through(filters, first=data)
            return
        key, values = PARTITION_FACETS[idx]
        if key in filters:
            recurse(filters, idx + 1, label)
            return
        for v in values:
            if stopped():
                return
            recurse({**filters, key: v}, idx + 1, f"{label} {key}={v}")

    db.update_job(job_id, status="running", message="partitioned course scrape starting",
                  db_path=db_path)
    log("Phase 1 (complete/partitioned): splitting the catalog to beat the pagination cap.")
    try:
        recurse({}, 0, "all")
        written = db.counts(db_path=db_path).get("courses", 0)
        reqs, byts, _ = stats.snapshot()
        if stopped():
            msg = f"stopped by user — {written}/{grand_total['v']} courses so far"
            status = "stopped"
        else:
            msg = f"done: {written}/{grand_total['v']} courses, {byts/1048576:.1f} MB"
            status = "completed"
        db.update_job(job_id, status=status, message=msg, finished_at=time.time(),
                      req_count=reqs, bytes_count=byts, db_path=db_path)
        log(msg)
        if status == "completed":
            send_notification(cfg, "Collegedunia Phase 1 complete", msg, log)
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
    """Phase 2 with: parallel proxy-pool fetching, request/bandwidth budgets,
    adaptive throttle + long cooldown, mid-course resume, skip-empty, ordering,
    scoping filters, and completion/block notifications."""
    import queue as _queue
    log = log or (lambda m: print(m, flush=True))
    pm = ProxyManager.from_config(cfg)
    stats = Stats()
    base_delay = float(cfg.get("delay", 1.0))
    adaptive = AdaptiveDelay(base_delay, enabled=bool(cfg.get("adaptive", True)))
    max_retries = int(cfg.get("max_retries", 5))
    backoff = float(cfg.get("backoff", 4))
    max_pages_per_course = cfg.get("max_pages_per_course")
    soft_retries = int(cfg.get("soft_block_retries", 4))
    long_cooldown = float(cfg.get("long_cooldown_seconds", 0))   # 0 = use short backoff
    concurrency = max(1, int(cfg.get("concurrency", 1)))
    skip_empty = bool(cfg.get("skip_empty", True))
    scope_filters = cfg.get("scope_filters") or {}
    budget_requests = int(cfg.get("budget_requests", 0))         # 0 = unlimited
    budget_mb = float(cfg.get("budget_mb", 0))                   # 0 = unlimited
    budget_bytes = int(budget_mb * 1024 * 1024)

    # ---- choose & order the courses to process ----
    course_ids: List[int] = cfg.get("course_ids") or []
    if not course_ids:
        where = cfg.get("course_where", "")
        params = tuple(cfg.get("course_where_params", []))
        course_ids = db.list_course_ids(db_path=db_path, where=where, params=params,
                                        order=cfg.get("order", "colleges_desc"))
    if skip_empty:
        nonzero = set(db.list_course_ids(db_path=db_path, where="colleges_count > 0"))
        if nonzero:
            course_ids = [c for c in course_ids if c in nonzero]
    if not cfg.get("force_rescrape"):
        done = db.get_done_course_ids(db_path=db_path)
        course_ids = [c for c in course_ids if c not in done]

    total_courses = len(course_ids)
    db.update_job(job_id, status="running", total_units=total_courses,
                  message=f"{total_courses} courses, concurrency={concurrency}", db_path=db_path)
    log(f"Phase 2: {total_courses} courses | concurrency={concurrency} | "
        f"budget {budget_requests or '∞'} reqs / {budget_mb or '∞'} MB | "
        f"adaptive={adaptive.enabled}")

    stop_event = threading.Event()
    db_lock = threading.Lock()
    prog_lock = threading.Lock()
    state = {"processed": 0, "offerings": 0, "incomplete": False, "msg": ""}

    def budget_hit() -> Optional[str]:
        reqs, byts, _ = stats.snapshot()
        if budget_requests and reqs >= budget_requests:
            return f"request budget reached ({reqs} ≥ {budget_requests})"
        if budget_bytes and byts >= budget_bytes:
            return f"bandwidth budget reached ({byts/1048576:.1f} ≥ {budget_mb} MB)"
        return None

    def push_progress() -> None:
        reqs, byts, blocks = stats.snapshot()
        with prog_lock:
            p, o = state["processed"], state["offerings"]
        with db_lock:
            db.update_job(job_id, done_units=p, items_written=o,
                          req_count=reqs, bytes_count=byts,
                          message=f"{p}/{total_courses} courses · {o} offerings · "
                                  f"{byts/1048576:.1f} MB · {blocks} blocks · "
                                  f"delay {adaptive.value():.1f}s", db_path=db_path)

    def scrape_course(cid: int, client: Client) -> int:
        prog = db.get_offering_progress(cid, db_path=db_path)
        start_page = 1
        course_total = prog.get("total_count") if prog else None
        if prog and prog.get("status") == "partial" and not cfg.get("force_rescrape"):
            start_page = max(1, int(prog.get("pages_done") or 0) + 1)
        page = start_page
        seen = (start_page - 1) * PAGE_SIZE
        local_off = 0
        while not stop_event.is_set():
            if db.stop_requested(job_id, db_path=db_path):
                stop_event.set(); return local_off
            bh = budget_hit()
            if bh:
                with prog_lock:
                    state["incomplete"] = True; state["msg"] = bh
                stop_event.set(); return local_off
            payload = {"page": page, "course_id": cid}
            payload.update(scope_filters)
            data = client.fetch(payload)
            rows = data.get("courses") or []
            if course_total is None:
                course_total = data.get("count")
            capped = bool(max_pages_per_course and page >= int(max_pages_per_course))
            reached_end = (course_total is not None and seen >= (course_total - PAGE_SIZE))
            if not rows and not reached_end and not capped:
                ok = False
                for attempt in range(1, soft_retries + 1):
                    if stop_event.is_set():
                        return local_off
                    wait = long_cooldown if long_cooldown else 10 * attempt
                    log(f"  ! course {cid} p{page} empty ({seen}/{course_total}) — "
                        f"retry {attempt}/{soft_retries} in {wait:.0f}s")
                    time.sleep(wait)
                    data = client.fetch(payload)
                    rows = data.get("courses") or []
                    if rows:
                        ok = True; break
                if not ok:
                    with db_lock:
                        db.set_offering_progress(cid, "partial", page - 1, course_total, db_path=db_path)
                    with prog_lock:
                        state["incomplete"] = True
                        state["msg"] = f"blocked on course {cid} page {page}"
                    stop_event.set(); return local_off
            if not rows:
                break
            colleges = [pc for pc in (parse_college(r) for r in rows) if pc]
            offerings = [parse_offering(cid, r) for r in rows]
            with db_lock:
                db.upsert_colleges(colleges, db_path=db_path)
                local_off += db.upsert_offerings(offerings, db_path=db_path)
                db.set_offering_progress(cid, "partial", page, course_total, db_path=db_path)
            seen += len(rows); page += 1
            if max_pages_per_course and page > int(max_pages_per_course):
                break
            if not data.get("hasNext", False):
                break
            time.sleep(adaptive.value())
        with db_lock:
            db.set_offering_progress(cid, "done", page - 1, course_total, db_path=db_path)
        return local_off

    q: "_queue.Queue" = _queue.Queue()
    for cid in course_ids:
        q.put(cid)

    def worker() -> None:
        client = Client(pm, log=log, max_retries=max_retries, backoff=backoff,
                        stats=stats, adaptive=adaptive)
        while not stop_event.is_set():
            try:
                cid = q.get_nowait()
            except _queue.Empty:
                return
            try:
                off = scrape_course(cid, client)
            except Exception as err:  # noqa: BLE001
                log(f"  course {cid} ERROR: {err}")
                off = 0
            with prog_lock:
                state["processed"] += 1
                state["offerings"] += off
            push_progress()

    try:
        threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        reqs, byts, blocks = stats.snapshot()
        if state["incomplete"]:
            msg = (f"INCOMPLETE — {state['msg']} ({state['processed']}/{total_courses} courses, "
                   f"{byts/1048576:.1f} MB). Resume to continue.")
            db.update_job(job_id, status="stopped", message=msg, finished_at=time.time(),
                          req_count=reqs, bytes_count=byts, db_path=db_path)
            log(msg); send_notification(cfg, "Collegedunia Phase 2 stopped", msg, log)
        elif db.stop_requested(job_id, db_path=db_path):
            msg = f"stopped by user after {state['processed']} courses"
            db.update_job(job_id, status="stopped", message=msg, finished_at=time.time(),
                          req_count=reqs, bytes_count=byts, db_path=db_path)
            log(msg)
        else:
            msg = (f"done: {state['processed']} courses, {state['offerings']} offerings, "
                   f"{byts/1048576:.1f} MB")
            db.update_job(job_id, status="completed", message=msg, finished_at=time.time(),
                          req_count=reqs, bytes_count=byts, db_path=db_path)
            log(msg); send_notification(cfg, "Collegedunia Phase 2 complete", msg, log)
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise
