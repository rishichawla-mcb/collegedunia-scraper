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
from collections import Counter
import html as _html
import itertools
import json
import random
import re
import threading
import time
from urllib.parse import urlparse, urlunparse
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


def sticky_gateway(url: str, session_id: Optional[str]) -> str:
    """Inject a DataImpulse-style sticky session id into a proxy gateway URL's
    username (``user;sessid.<id>``) so all requests in a session use one IP.
    Harmless for providers that ignore the suffix."""
    if not url or not session_id:
        return url
    try:
        p = urlparse(url)
        if not p.username:
            return url
        user = f"{p.username};sessid.{session_id}"
        auth = user + (f":{p.password}" if p.password else "")
        host = p.hostname or ""
        netloc = f"{auth}@{host}" + (f":{p.port}" if p.port else "")
        return urlunparse((p.scheme, netloc, p.path, p.params, p.query, p.fragment))
    except Exception:
        return url


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

    def get(self, session_id: Optional[str] = None) -> Optional[Proxy]:
        if self.mode == "none":
            return None
        if self.mode == "gateway":
            if not self.gateway:
                return None
            return Proxy(url=sticky_gateway(self.gateway, session_id) if session_id else self.gateway)
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
        self.session_id: Optional[str] = None  # set for sticky-IP pagination
        self.verbose = True                    # per-request live logging (bounded by log prune)

    def _maybe_rotate(self, err: Exception) -> bool:
        """A dead proxy tunnel (502 NO_HOST_CONNECTION / 'Unable to connect to
        proxy') won't recover by retrying the SAME sticky IP. On a proxy
        connection error, rotate the sticky session id so the next attempt gets
        a fresh upstream IP. Site blocks (403/429) keep the same IP."""
        is_proxy_err = isinstance(err, requests.exceptions.ProxyError) or (
            isinstance(err, requests.exceptions.ConnectionError)
            and ("NO_HOST_CONNECTION" in str(err) or "Unable to connect to proxy" in str(err)))
        if self.session_id and is_proxy_err:
            base = self.session_id.split("~")[0]
            self.session_id = f"{base}~{random.randint(0, 10**6)}"
            return True
        return False

    def fetch(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        params = {"data": encode_payload(payload)}
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            proxy = self.pm.get(self.session_id)
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
                if self.verbose:
                    self.log(f"   · page {payload.get('page')} → "
                             f"{len(data.get('courses') or [])} rows, "
                             f"{len(resp.content or b'')//1024} KB "
                             f"(filters: {[k for k in payload if k != 'page']})")
                return data
            except (BlockedError, requests.RequestException, ValueError) as err:
                last_err = err
                self.stats.add(blocks=1)
                self.pm.report_failure(proxy)
                rotated = self._maybe_rotate(err)
                # Proxy-tunnel errors get a fresh IP, but must NOT ramp the
                # adaptive throttle — that's only for real site rate-limiting.
                if self.adaptive and not rotated:
                    self.adaptive.on_block()
                wait = self.backoff * attempt + random.uniform(0, 2)
                via = proxy.url if proxy else "direct"
                self.log(f"  ! attempt {attempt}/{self.max_retries} via {via} failed: "
                         f"{err}{' [rotating to fresh proxy IP]' if rotated else ''} "
                         f"-> retry in {wait:.0f}s")
                time.sleep(wait)
        raise RuntimeError(f"Failed after {self.max_retries} attempts: {last_err}")

    def get_text(self, url: str) -> str:
        """GET an HTML page (for Phase-3 enrichment) through the proxy, with the
        same retry/rotation/block handling. Returns the response text."""
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            proxy = self.pm.get(self.session_id)
            try:
                resp = self.session.get(
                    url, headers=base_headers(),
                    proxies=proxy.as_dict() if proxy else None, timeout=self.timeout)
                self.stats.add(requests=1, byts=len(resp.content or b""))
                if resp.status_code in (403, 429, 503):
                    raise BlockedError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                self.pm.report_success(proxy)
                if self.adaptive:
                    self.adaptive.on_success()
                if self.verbose:
                    self.log(f"   · GET …{url[-46:]} → {len(resp.content or b'')//1024} KB")
                return resp.text
            except (BlockedError, requests.RequestException) as err:
                last_err = err
                self.stats.add(blocks=1)
                self.pm.report_failure(proxy)
                if not self._maybe_rotate(err) and self.adaptive:
                    self.adaptive.on_block()
                time.sleep(self.backoff * attempt + random.uniform(0, 2))
        raise RuntimeError(f"GET failed after {self.max_retries} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Phase-3: parse the CollegeOrUniversity JSON-LD from a college page (no reviews)
# ---------------------------------------------------------------------------
_LD_RE = re.compile(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                    re.DOTALL | re.IGNORECASE)


def parse_college_ld(html: str) -> Dict[str, Any]:
    """Extract structured college fields from the page's JSON-LD. Deliberately
    ignores UserComments/reviews."""
    best: Dict[str, Any] = {}
    for block in _LD_RE.findall(html or ""):
        try:
            obj = json.loads(block.strip())
        except (json.JSONDecodeError, ValueError):
            continue
        items = obj if isinstance(obj, list) else [obj]
        for o in items:
            if not isinstance(o, dict) or o.get("@type") != "CollegeOrUniversity":
                continue
            agg = o.get("aggregateRating") or {}
            addr = o.get("address")
            if isinstance(addr, dict):
                addr = ", ".join(str(addr.get(k, "")) for k in
                                 ("streetAddress", "addressLocality", "addressRegion",
                                  "postalCode", "addressCountry") if addr.get(k))
            best = {
                "website": o.get("url", ""),
                "email": o.get("email", ""),
                "phone": o.get("telephone", ""),
                "rating_value": _to_float(agg.get("ratingValue")),
                "rating_count": _to_int(agg.get("ratingCount") or agg.get("reviewCount")),
                "pros": (o.get("positiveNotes") or "")[:2000] if isinstance(o.get("positiveNotes"), str) else "",
                "cons": (o.get("negativeNotes") or "")[:2000] if isinstance(o.get("negativeNotes"), str) else "",
                "address": addr if isinstance(addr, str) else "",
            }
            return best
    return best


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
# Live / generic extraction — point at any URL, inspect its structure, and
# pull elements by CSS class or a custom selector. Powers the "Live scraper" UI.
# ---------------------------------------------------------------------------
def _make_soup(html: str):
    """Lazily import BeautifulSoup so the app still boots if the optional
    'beautifulsoup4' dependency isn't installed — only the live scraper needs it."""
    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "The live scraper needs the 'beautifulsoup4' package, which isn't "
            "installed. Add 'beautifulsoup4' to requirements.txt and redeploy."
        ) from exc
    return BeautifulSoup(html or "", "html.parser")


def analyze_page(html: str, top: int = 500) -> Dict[str, Any]:
    """Inventory a page so the user can decide what to extract. Returns
    {'title', 'classes': [{class, count, tags, sample}], 'tags': {tag: count}}."""
    soup = _make_soup(html)
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    class_info: Dict[str, Dict[str, Any]] = {}
    tag_counts: "Counter" = Counter()
    for el in soup.find_all(True):
        tag_counts[el.name] += 1
        for cls in (el.get("class") or []):
            info = class_info.setdefault(
                cls, {"class": cls, "count": 0, "tags": set(), "sample": ""})
            info["count"] += 1
            info["tags"].add(el.name)
            if not info["sample"]:
                txt = el.get_text(" ", strip=True)
                if txt:
                    info["sample"] = txt[:140]
    classes = sorted(class_info.values(), key=lambda d: d["count"], reverse=True)[:top]
    for c in classes:
        c["tags"] = ", ".join(sorted(c["tags"]))
    return {"title": title, "classes": classes, "tags": dict(tag_counts.most_common(80))}


def _el_to_row(el, mode: str) -> Dict[str, Any]:
    """Flatten one matched element into a row, per the chosen extraction mode."""
    row: Dict[str, Any] = {"tag": el.name, "text": el.get_text(" ", strip=True)}
    if mode == "links":
        href = el.get("href", "")
        if not href:
            a = el.find("a", href=True)
            href = a.get("href", "") if a else ""
        src = el.get("src", "")
        if not src:
            img = el.find("img", src=True)
            src = img.get("src", "") if img else ""
        row["href"] = abs_url(href) if href else ""
        row["src"] = src or ""
    elif mode == "html":
        row["html"] = el.decode_contents()
    return row


def extract_by_selector(html: str, selector: str, mode: str = "text",
                        limit: int = 5000) -> List[Dict[str, Any]]:
    """Extract elements matching a CSS selector. mode: 'text' | 'links' | 'html'."""
    soup = _make_soup(html)
    return [_el_to_row(el, mode) for el in soup.select(selector)[:limit]]


def extract_by_classes(html: str, classes: List[str], mode: str = "text",
                       limit: int = 5000) -> List[Dict[str, Any]]:
    """Extract every element carrying any of the given classes (robust to odd
    class names that a raw CSS selector would choke on)."""
    soup = _make_soup(html)
    want = set(classes)
    out: List[Dict[str, Any]] = []
    for el in soup.find_all(True):
        if want & set(el.get("class") or []):
            out.append(_el_to_row(el, mode))
            if len(out) >= limit:
                break
    return out


# ---------------------------------------------------------------------------
# Phase 1: courses
# ---------------------------------------------------------------------------
# Facets used to partition the course list under the API's ~1,700-result
# pagination ceiling. Order matters: broad → narrow.
COURSE_TYPE_VALUES = ["Degree", "Diploma", "Certification"]
LEVEL_VALUES = ["Graduation", "Post Graduation", "10+2", "Doctorate/M.Phil", "10th"]
# NOTE: the `stream` filter breaks deep pagination on this API (results go empty
# after ~2 pages). `course_tag_id`, `level`, and `course_type` paginate properly,
# so we partition by those instead. Tag ids are discovered at runtime.
SUB_FACETS = [("level", LEVEL_VALUES), ("course_type", COURSE_TYPE_VALUES)]
TAG_ID_RANGE = range(1, 231)
PAGINATION_CAP = 1500   # split a slice finer if its count exceeds this
MAX_EMPTY_PAGES = 6     # stop a chunk after this many consecutive empty pages
MAX_CHUNK_PAGES = 400   # hard safety cap on pages per chunk


_UPSERT_MAP = {
    "courses": "upsert_courses", "colleges": "upsert_colleges",
    "offerings": "upsert_offerings", "college_courses": "upsert_college_courses",
}


def _write_rows(job_id: int, cfg: Dict[str, Any], table: str,
                rows: List[Dict[str, Any]], db_path: str) -> int:
    """Route a runner's output: to per-job staging (default) or straight to
    master (fallback when cfg['staging'] is False)."""
    if not rows:
        return 0
    if cfg.get("staging", True):
        return db.stage_records(job_id, table, rows, db_path=db_path)
    return getattr(db, _UPSERT_MAP[table])(rows, db_path=db_path)


def _staged_or_master_count(job_id: int, cfg: Dict[str, Any], db_path: str) -> int:
    if cfg.get("staging", True):
        return sum(db.staged_summary(job_id, db_path=db_path).values())
    return sum(db.counts(db_path=db_path).values())


def _finalize_job(job_id: int, cfg: Dict[str, Any], log: Callable[[str], None],
                  base_msg: str, db_path: str) -> None:
    """Validate a job's staged data; auto-promote if it passes, else leave it
    staged and pending manual approval. No-op (just mark completed) when staging
    is off."""
    if not cfg.get("staging", True):
        db.update_job(job_id, status="completed", message=base_msg,
                      finished_at=time.time(), db_path=db_path)
        return
    v = db.validate_job(job_id, cfg.get("validation_rules") or {}, db_path=db_path)
    staged = sum(db.staged_summary(job_id, db_path=db_path).values())
    db.update_job(job_id, quality_score=v["score"], staged_rows=staged, db_path=db_path)
    auto = cfg.get("auto_promote", True)
    if v["passed"] and auto:
        summ = db.promote_job(job_id, db_path=db_path)
        msg = f"{base_msg} · QC {v['score']:.0f}/100 ✓ promoted ({sum(summ.values())} rows)"
        db.update_job(job_id, status="completed", promote_status="promoted",
                      message=msg, finished_at=time.time(), db_path=db_path)
        log(msg)
        send_notification(cfg, "Collegedunia job promoted", msg, log)
    else:
        why = "failed QC" if not v["passed"] else "auto-promote off"
        msg = f"{base_msg} · QC {v['score']:.0f}/100 — staged, awaiting approval ({why})"
        db.update_job(job_id, status="completed", promote_status="pending",
                      message=msg, finished_at=time.time(), db_path=db_path)
        log(msg)
        send_notification(cfg, "Collegedunia job needs review", msg, log)


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
        written = (db.staged_summary(job_id, db_path=db_path).get("courses", 0) if cfg.get("staging", True) else db.counts(db_path=db_path).get("courses", 0))
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
            _write_rows(job_id, cfg, "courses", parsed, db_path)
            written = (db.staged_summary(job_id, db_path=db_path).get("courses", 0) if cfg.get("staging", True) else db.counts(db_path=db_path).get("courses", 0))
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
        written = (db.staged_summary(job_id, db_path=db_path).get("courses", 0) if cfg.get("staging", True) else db.counts(db_path=db_path).get("courses", 0))
        db.update_job(job_id, done_units=written, items_written=written,
                      total_units=(grand_total["v"] or 0), req_count=reqs, bytes_count=byts,
                      message=f"{written}/{grand_total['v']} courses · {leaves_done['v']} chunks · "
                              f"{byts/1048576:.1f} MB", db_path=db_path)

    def page_through(filters: Dict[str, Any], first: Optional[Dict[str, Any]],
                     expected: int = 0) -> None:
        """Page a slice, following its (often sparse/degrading) tail: keep going
        until MAX_EMPTY_PAGES consecutive empty pages, capturing every straggler.
        Anchoring on `count` doesn't work because the API rarely serves it all."""
        page = 1
        data = first
        consec_empty = 0
        while not stopped() and page <= MAX_CHUNK_PAGES:
            if data is None:
                data = fetch_page(filters, page)
            rows = data.get("courses") or []
            if rows:
                consec_empty = 0
                parsed = [parse_course(c) for c in rows
                          if (c.get("lead_params") or {}).get("course_id")]
                _write_rows(job_id, cfg, "courses", parsed, db_path)
            else:
                consec_empty += 1
                if consec_empty >= MAX_EMPTY_PAGES:
                    break
            page += 1
            data = None
            if page % 10 == 0:
                push()
            time.sleep(adaptive.value())
        leaves_done["v"] += 1
        push()

    def recurse(filters: Dict[str, Any], idx: int, label: str) -> None:
        if stopped():
            return
        # one sticky IP per slice so pagination stays consistent
        client.session_id = f"cf{random.randint(0, 10**9)}"
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
        if count <= PAGINATION_CAP or idx >= len(facets):
            log(f"  → scraping chunk '{label}' (count {count})")
            page_through(filters, first=data, expected=count)
            return
        key, values = facets[idx]
        if key in filters:
            recurse(filters, idx + 1, label)
            return
        for v in values:
            if stopped():
                return
            recurse({**filters, key: v}, idx + 1, f"{label} {key}={v}")

    db.update_job(job_id, status="running", message="discovering course tags…",
                  db_path=db_path)
    log("Phase 1 (complete/partitioned) [BUILD: tagid-v5] — "
        "partitioning by course_tag_id (stream breaks deep pagination).")
    # Discover valid course_tag_id values (rotate IP per probe).
    tag_ids: List[int] = []
    for t in TAG_ID_RANGE:
        if stopped():
            break
        client.session_id = f"d{t}"
        try:
            d = fetch_page({"course_tag_id": t}, 1)
            if (d.get("count") or 0) > 0:
                tag_ids.append(t)
        except Exception:  # noqa: BLE001
            pass
        time.sleep(adaptive.value())
    log(f"Discovered {len(tag_ids)} course tags.")
    facets = [("course_tag_id", tag_ids)] + SUB_FACETS
    try:
        recurse({}, 0, "all")
        written = (db.staged_summary(job_id, db_path=db_path).get("courses", 0) if cfg.get("staging", True) else db.counts(db_path=db_path).get("courses", 0))
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
        if status == "completed" and not cfg.get("_pipeline"):
            send_notification(cfg, "Collegedunia Phase 1 complete", msg, log)
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise


def run_pipeline(job_id: int, cfg: Dict[str, Any], db_path: str = db.DB_PATH,
                 log: Optional[Callable[[str], None]] = None) -> None:
    """One-click full pipeline: complete partitioned Phase 1, then Phase 2 over
    every course — on a single job."""
    log = log or (lambda m: print(m, flush=True))
    log("=== PIPELINE: Phase 1 (courses) ===")
    _run_courses_partitioned(job_id, {**cfg, "partition": True, "_pipeline": True}, db_path, log)
    if db.stop_requested(job_id, db_path=db_path):
        return
    job = db.get_job(job_id, db_path=db_path)
    if job and job["status"] == "error":
        return
    log("=== PIPELINE: Phase 2 (colleges per course) ===")
    db.update_job(job_id, status="running", message="phase 1 done — starting phase 2",
                  db_path=db_path)
    run_offerings(job_id, {**cfg, "_pipeline": True}, db_path, log)


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
    state = {"processed": 0, "offerings": 0, "blocked": 0, "incomplete": False, "msg": ""}

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
        client.session_id = f"crs{cid}"   # one sticky IP per course's pagination
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
                    # One stubbornly-empty/blocked course shouldn't kill the whole
                    # run. Mark it 'partial' (so a later resume / next non-force run
                    # retries just this course) and move on. Only abort the entire
                    # job if MANY courses block while nothing is coming through —
                    # that signals a global IP throttle, not a single bad course.
                    with db_lock:
                        db.set_offering_progress(cid, "partial", page - 1, course_total, db_path=db_path)
                    with prog_lock:
                        state["blocked"] += 1
                        global_block = state["blocked"] >= 8 and state["offerings"] == 0
                        if global_block:
                            state["incomplete"] = True
                            state["msg"] = f"repeated blocks (e.g. course {cid}) — likely IP-throttled"
                    if global_block:
                        stop_event.set()
                    return local_off
            if not rows:
                break
            colleges = [pc for pc in (parse_college(r) for r in rows) if pc]
            offerings = [parse_offering(cid, r) for r in rows]
            with db_lock:
                _write_rows(job_id, cfg, "colleges", colleges, db_path)
                local_off += _write_rows(job_id, cfg, "offerings", offerings, db_path)
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
            blocked = state.get("blocked", 0)
            extra = f" · {blocked} course(s) blocked, will retry on next run/resume" if blocked else ""
            msg = (f"done: {state['processed']} courses, {state['offerings']} offerings, "
                   f"{byts/1048576:.1f} MB{extra}")
            db.update_job(job_id, status="completed", message=msg, finished_at=time.time(),
                          req_count=reqs, bytes_count=byts, db_path=db_path)
            log(msg); send_notification(cfg, "Collegedunia Phase 2 complete", msg, log)
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise


# ---------------------------------------------------------------------------
# Phase 3: per-college enrichment (JSON-LD: website/email/phone/rating/pros/cons/address)
# ---------------------------------------------------------------------------
def run_enrichment(job_id: int, cfg: Dict[str, Any], db_path: str = db.DB_PATH,
                   log: Optional[Callable[[str], None]] = None) -> None:
    import queue as _queue
    log = log or (lambda m: print(m, flush=True))
    pm = ProxyManager.from_config(cfg)
    stats = Stats()
    adaptive = AdaptiveDelay(float(cfg.get("delay", 1.0)), enabled=bool(cfg.get("adaptive", True)))
    concurrency = max(1, int(cfg.get("concurrency", 1)))
    budget_bytes = int(float(cfg.get("budget_mb", 0)) * 1024 * 1024)
    budget_requests = int(cfg.get("budget_requests", 0))

    colleges = db.list_colleges_to_enrich(
        db_path=db_path, where=cfg.get("college_where", ""),
        params=tuple(cfg.get("college_where_params", [])),
        include_done=bool(cfg.get("force_rescrape")), limit=cfg.get("limit"))
    total = len(colleges)
    db.update_job(job_id, status="running", total_units=total,
                  message=f"{total} colleges to enrich", db_path=db_path)
    log(f"Phase 3: enriching {total} colleges, concurrency={concurrency}")

    stop_event = threading.Event()
    db_lock = threading.Lock()
    prog_lock = threading.Lock()
    state = {"done": 0, "ok": 0, "incomplete": False, "msg": ""}

    def budget_hit():
        reqs, byts, _ = stats.snapshot()
        if budget_requests and reqs >= budget_requests:
            return f"request budget reached ({reqs})"
        if budget_bytes and byts >= budget_bytes:
            return f"bandwidth budget reached ({byts/1048576:.1f} MB)"
        return None

    def push():
        reqs, byts, _ = stats.snapshot()
        with prog_lock:
            d, ok = state["done"], state["ok"]
        with db_lock:
            db.update_job(job_id, done_units=d, items_written=ok, req_count=reqs,
                          bytes_count=byts,
                          message=f"{d}/{total} colleges · {ok} enriched · {byts/1048576:.1f} MB",
                          db_path=db_path)

    q: "_queue.Queue" = _queue.Queue()
    for cobj in colleges:
        q.put(cobj)

    def worker():
        client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                        backoff=float(cfg.get("backoff", 4)), stats=stats, adaptive=adaptive)
        while not stop_event.is_set():
            try:
                cobj = q.get_nowait()
            except _queue.Empty:
                return
            if db.stop_requested(job_id, db_path=db_path):
                stop_event.set(); return
            bh = budget_hit()
            if bh:
                with prog_lock:
                    state["incomplete"] = True; state["msg"] = bh
                stop_event.set(); return
            ok = False
            try:
                html = client.get_text(cobj["link"])
                fields = parse_college_ld(html)
                with db_lock:
                    db.update_college_details(cobj["college_id"], fields, db_path=db_path)
                ok = bool(fields)
            except Exception as err:  # noqa: BLE001
                log(f"  college {cobj['college_id']} err: {str(err)[:60]}")
            with prog_lock:
                state["done"] += 1
                if ok:
                    state["ok"] += 1
                d = state["done"]
            if d % 5 == 0:
                push()
            time.sleep(adaptive.value())

    try:
        threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        reqs, byts, _ = stats.snapshot()
        if state["incomplete"]:
            msg = (f"INCOMPLETE — {state['msg']} ({state['done']}/{total}, "
                   f"{byts/1048576:.1f} MB). Resume to continue.")
            status = "stopped"
        elif db.stop_requested(job_id, db_path=db_path):
            msg = f"stopped by user after {state['done']} colleges"
            status = "stopped"
        else:
            msg = f"done: {state['ok']}/{total} colleges enriched, {byts/1048576:.1f} MB"
            status = "completed"
        db.update_job(job_id, status=status, message=msg, finished_at=time.time(),
                      req_count=reqs, bytes_count=byts, db_path=db_path)
        log(msg)
        if status == "completed":
            send_notification(cfg, "Collegedunia Phase 3 complete", msg, log)
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise


# ---------------------------------------------------------------------------
# Phase 4: per-college courses & fees (parse the SSR fees table)
# ---------------------------------------------------------------------------
_TABLE_RE = re.compile(r"<table\b[^>]*>(.*?)</table>", re.S | re.I)
_TR_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
_TD_RE = re.compile(r"<t[hd]\b[^>]*>(.*?)</t[hd]>", re.S | re.I)


def _clean_html(s: str) -> str:
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", _html.unescape(s)).strip()


def _clean_fee(s: str) -> str:
    """Normalise a fee cell like '894(1st Year Fees)' -> '₹894 (1st Year Fees)'
    and '₹ 2.65 Lakhs' -> '₹2.65 Lakhs'."""
    s = (s or "").strip()
    if not s or s in ("-", "—", "N/A", "NA", "--"):
        return ""
    s = re.sub(r"\s*\(", " (", s)              # normalise space before a '('
    s = re.sub(r"₹\s+", "₹", s)                # tighten '₹ 1.41' -> '₹1.41'
    s = re.sub(r"\s+", " ", s).strip()
    if re.match(r"^[\d,]", s):                  # bare number -> prefix ₹
        s = "₹" + s
    return s


def _clean_date(d: Any) -> str:
    d = str(d or "").strip()
    return "" if d in ("", "0000-00-00") else d


_NEXTDATA_RE = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)


def _hostel_from_tables(page_html: str) -> str:
    """College-level hostel fee isn't in the JSON; grab the first non-empty
    'Hostel Fees' cell from the page's HTML tables."""
    for tbl in _TABLE_RE.findall(page_html or ""):
        rows = _TR_RE.findall(tbl)
        if not rows:
            continue
        hdr = [_clean_html(h).lower() for h in _TD_RE.findall(rows[0])]
        hi = next((i for i, h in enumerate(hdr) if "hostel" in h), None)
        if hi is None:
            continue
        for r in rows[1:]:
            cells = [_clean_html(c) for c in _TD_RE.findall(r)]
            if hi < len(cells):
                v = _clean_fee(cells[hi])
                if v:
                    return v
    return ""


def parse_courses_fees(page_html: str) -> Dict[str, Any]:
    """Phase 4 parser. Primary source is the page's embedded __NEXT_DATA__ JSON
    (clean and rich: fee, duration, mode, level, eligibility, rating, reviews,
    specialization, application dates). Hostel fee — which is college-level and
    not in the JSON — is merged from the HTML table. Falls back to HTML-table
    parsing when the JSON is absent."""
    m = _NEXTDATA_RE.search(page_html or "")
    if m:
        clist = None
        cd: Dict[str, Any] = {}
        college_name = ""
        try:
            data = json.loads(m.group(1))
            pdata = data["props"]["initialProps"]["pageProps"]["data"]
            cd = pdata.get("course_data") or {}
            clist = cd.get("courses") or []
            college_name = pdata.get("college_name") or ""
        except Exception:  # noqa: BLE001
            clist = None
        if clist:
            hostel = _hostel_from_tables(page_html)
            courses: List[Dict[str, Any]] = []
            seen = set()
            for c in clist:
                base = (c.get("display_name") or c.get("short_head") or "").strip()
                if not base:
                    continue
                for s in (c.get("streams") or [{}]):
                    spec = (s.get("name") or "").strip()
                    fd = s.get("fees_data") or {}
                    amt_fmt = fd.get("amount_formatted") or ""
                    adm = s.get("admission") or {}
                    is_spec = bool(spec) and spec.lower() != "general"
                    full = f"{base} ({spec})" if is_spec else base
                    key = full.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    courses.append({
                        "course_name": full,
                        "specialization": spec if is_spec else "",
                        "eligibility": (c.get("eligibility") or "").strip(),
                        "total_fees": ("₹" + amt_fmt) if amt_fmt else "",
                        "fees_inr": _to_int(fd.get("amount")),
                        "hostel_fees": hostel,
                        "duration": (c.get("duration") or "").strip(),
                        "mode": c.get("type") or "",
                        "level": c.get("level") or "",
                        "course_type": c.get("course_type") or "",
                        "rating": _to_float(c.get("course_rating")),
                        "reviews_count": _to_int(c.get("reviews_count")),
                        "application_start": _clean_date(adm.get("admission_start_date")),
                        "application_end": _clean_date(adm.get("admission_end_date")),
                    })
            return {"college_name": college_name, "courses": courses,
                    "course_count": cd.get("course_count"),
                    "total_pages": cd.get("total_pages")}
    return _parse_cf_tables(page_html)


def _parse_cf_tables(page_html: str) -> Dict[str, Any]:
    """Legacy fallback: parse the HTML 'Courses & Fees' table(s) when no
    __NEXT_DATA__ JSON is present. Only accepts tables that have BOTH a
    course/program column and a fee column — this skips the fee-breakdown
    ('Fee Type | Amount'), yearly-fee, and scholarship tables that the old
    parser mistook for course rows."""
    name = ""
    mt = re.search(r"<title>(.*?)</title>", page_html or "", re.S | re.I)
    if mt:
        name = _clean_html(mt.group(1)).split(":")[0].split(" Courses")[0].strip()
    courses: List[Dict[str, Any]] = []
    seen = set()
    for tbl in _TABLE_RE.findall(page_html or ""):
        rows = _TR_RE.findall(tbl)
        if len(rows) < 2:
            continue
        hdr = [_clean_html(h).lower() for h in _TD_RE.findall(rows[0])]
        if not hdr:
            continue

        def col(*keys):
            for i, h in enumerate(hdr):
                if all(k in h for k in keys):
                    return i
            return None

        def first_col(*opts):
            for o in opts:
                keys = o if isinstance(o, tuple) else (o,)
                idx = col(*keys)
                if idx is not None:
                    return idx
            return None

        ci_course = first_col("course", "program", "specialization", "branch")
        ci_total = first_col(("total", "fee"), ("annual", "fee"), ("1st", "fee"), "fee", "fees")
        if ci_course is None or ci_total is None or ci_course == ci_total:
            continue
        ci_elig = col("eligib")
        ci_hostel = col("hostel")
        for r in rows[1:]:
            cells = [_clean_html(c) for c in _TD_RE.findall(r)]
            if not cells or not any(cells):
                continue

            def g(i):
                return cells[i] if (i is not None and i < len(cells)) else ""

            cn = g(ci_course).strip()
            cnl = cn.lower()
            if (not cn or len(cn) > 150 or cnl in seen or "fee" in cnl
                    or "college" in cnl or "university" in cnl
                    or re.match(r"^[₹\d]", cn)):
                continue
            seen.add(cnl)
            courses.append({"course_name": cn, "eligibility": g(ci_elig).strip(),
                            "total_fees": _clean_fee(g(ci_total)),
                            "hostel_fees": _clean_fee(g(ci_hostel))})
    return {"college_name": name, "courses": courses}


def run_college_courses(job_id: int, cfg: Dict[str, Any], db_path: str = db.DB_PATH,
                        log: Optional[Callable[[str], None]] = None) -> None:
    """Phase 4: for each college id, fetch /college/<id>/courses-fees and store
    its course+fee rows. IDs come from the known colleges table and/or an
    explicit id range (cfg id_start/id_end). The id alone resolves (the site
    redirects to the canonical slug)."""
    import queue as _queue
    log = log or (lambda m: print(m, flush=True))
    pm = ProxyManager.from_config(cfg)
    stats = Stats()
    adaptive = AdaptiveDelay(float(cfg.get("delay", 1.0)), enabled=bool(cfg.get("adaptive", True)))
    concurrency = max(1, int(cfg.get("concurrency", 1)))
    budget_bytes = int(float(cfg.get("budget_mb", 0)) * 1024 * 1024)
    budget_requests = int(cfg.get("budget_requests", 0))

    ids: List[int] = list(cfg.get("college_ids") or [])
    if cfg.get("use_known", True) and not ids:
        ids = db.list_known_college_ids(db_path=db_path)
    if cfg.get("id_start") and cfg.get("id_end"):
        ids = sorted(set(ids) | set(range(int(cfg["id_start"]), int(cfg["id_end"]) + 1)))
    if not cfg.get("force_rescrape"):
        done = db.get_cc_done_ids(db_path=db_path)
        ids = [i for i in ids if i not in done]

    total = len(ids)
    db.update_job(job_id, status="running", total_units=total,
                  message=f"{total} colleges' courses-fees to scrape", db_path=db_path)
    log(f"Phase 4 [BUILD: ccfees-v1]: {total} colleges, concurrency={concurrency}")

    stop_event = threading.Event()
    db_lock = threading.Lock()
    prog_lock = threading.Lock()
    state = {"done": 0, "rows": 0, "incomplete": False, "msg": ""}

    def budget_hit():
        reqs, byts, _ = stats.snapshot()
        if budget_requests and reqs >= budget_requests:
            return f"request budget reached ({reqs})"
        if budget_bytes and byts >= budget_bytes:
            return f"bandwidth budget reached ({byts/1048576:.1f} MB)"
        return None

    def push():
        reqs, byts, _ = stats.snapshot()
        with prog_lock:
            d, rw = state["done"], state["rows"]
        with db_lock:
            db.update_job(job_id, done_units=d, items_written=rw, req_count=reqs,
                          bytes_count=byts,
                          message=f"{d}/{total} colleges · {rw} course-rows · {byts/1048576:.1f} MB",
                          db_path=db_path)

    q: "_queue.Queue" = _queue.Queue()
    for i in ids:
        q.put(i)

    def worker():
        client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                        backoff=float(cfg.get("backoff", 4)), stats=stats, adaptive=adaptive)
        while not stop_event.is_set():
            try:
                cid = q.get_nowait()
            except _queue.Empty:
                return
            if db.stop_requested(job_id, db_path=db_path):
                stop_event.set(); return
            bh = budget_hit()
            if bh:
                with prog_lock:
                    state["incomplete"] = True; state["msg"] = bh
                stop_event.set(); return
            client.session_id = f"cc{cid}"
            url = f"{SITE}/college/{cid}/courses-fees"
            n = 0
            try:
                parsed = parse_courses_fees(client.get_text(url))
                rows = [{**r, "college_id": cid, "college_name": parsed.get("college_name", ""),
                         "source_url": url, "scraped_at": time.time()}
                        for r in parsed.get("courses", [])]
                with db_lock:
                    n = _write_rows(job_id, cfg, "college_courses", rows, db_path)
                    db.set_cc_progress(cid, "done" if n else "empty", n, db_path=db_path)
            except Exception as err:  # noqa: BLE001
                with db_lock:
                    db.set_cc_progress(cid, "error", 0, db_path=db_path)
                log(f"  college {cid} err: {str(err)[:60]}")
            with prog_lock:
                state["done"] += 1
                state["rows"] += n
                d = state["done"]
            if d % 5 == 0:
                push()
            time.sleep(adaptive.value())

    try:
        threads = [threading.Thread(target=worker, daemon=True) for _ in range(concurrency)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        reqs, byts, _ = stats.snapshot()
        if state["incomplete"]:
            msg = (f"INCOMPLETE — {state['msg']} ({state['done']}/{total}, "
                   f"{byts/1048576:.1f} MB). Resume to continue.")
            status = "stopped"
        elif db.stop_requested(job_id, db_path=db_path):
            msg = f"stopped by user after {state['done']} colleges"
            status = "stopped"
        else:
            msg = f"done: {state['done']} colleges, {state['rows']} course-rows, {byts/1048576:.1f} MB"
            status = "completed"
        db.update_job(job_id, status=status, message=msg, finished_at=time.time(),
                      req_count=reqs, bytes_count=byts, db_path=db_path)
        log(msg)
        if status == "completed":
            send_notification(cfg, "Collegedunia Phase 4 complete", msg, log)
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise
