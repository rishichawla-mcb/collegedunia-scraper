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

BUILD = "2026-07-23a"  # keep in sync across app/db/scraper/export (header checks this)

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
COURSES_LIST_API = "https://collegedunia.com/web-api/college/courses-list"
LISTING_API = "https://collegedunia.com/web-api/listing"
SITE = "https://collegedunia.com"
PAGE_SIZE = 10
CC_PAGE_SIZE = 5          # courses-list API returns 5 course groups per page
MAX_CC_PAGES = 60         # hard safety cap on course pages per college
DIR_PAGE_SIZE = 10        # listing API returns 10 colleges per page
MAX_DIR_SLUG_PAGES = 400  # hard cap per state partition (~4,000 colleges)
DIR_BASE_SWEEP_CAP = 998  # india-colleges base sweep stops before the ~999 ceiling

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


def courses_list_payload(college_id: Any, page: Any) -> str:
    """Base64 payload for the courses-list pagination API. Both fields must be
    strings: {"id": "<college_id>", "course_page": "<n>"}."""
    return encode_payload({"id": str(college_id), "course_page": str(page)})


def listing_payload(slug: str, page: int) -> str:
    """Base64 payload for the india-colleges directory listing API:
    {"url": "<listing-slug>", "page": <int>} (page is an int here)."""
    return encode_payload({"url": str(slug), "page": int(page)})


def iter_course_pages(fetch_page: "Callable[[int], Dict[str, Any]]",
                      total_pages: int, start_page: int = 2,
                      max_pages: int = MAX_CC_PAGES):
    """Yield (page, data) for course-list API pages start_page..total_pages,
    stopping as soon as a page reports hasNext=False. `total_pages` caps the
    loop; `max_pages` is a hard safety cap. `fetch_page(page)` returns the API
    JSON dict. Pure/generator so it's unit-testable without network."""
    cap = min(int(total_pages or 0), int(max_pages))
    page = int(start_page)
    while page <= cap:
        data = fetch_page(page)
        yield page, data
        if not (data or {}).get("hasNext", False):
            break
        page += 1


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


# Bot-interstitials return HTTP 200 with an HTML body, so status codes alone
# don't catch them. fetch() spots them because it expects JSON; get_text() had no
# equivalent check, so Phase 3 / Phase 4 / the Directory HTML fallback silently
# parsed challenge pages as if they were real (a blocked college was then written
# with NULL contacts and permanently marked 'enriched'). These markers are
# deliberately narrow — the title-based ones and the Cloudflare/Incapsula tokens
# do not occur in genuine Collegedunia markup.
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.S | re.I)
_CHALLENGE_TITLES = ("just a moment", "attention required", "access denied",
                     "security check", "are you a robot", "ddos-guard")
_CHALLENGE_TOKENS = ("cf-browser-verification", "__cf_chl_", "cf_chl_opt",
                     "request unsuccessful. incapsula",
                     "please enable cookies and reload the page")


def is_block_page(text: Optional[str]) -> bool:
    """True when an HTTP-200 body is really a bot challenge / interstitial."""
    if text is None:
        return True
    head = text[:20000]
    if not head.strip():
        return True                      # empty 200 is not a real page either
    m = _TITLE_RE.search(head)
    if m:
        title = m.group(1).strip().lower()
        if any(t in title for t in _CHALLENGE_TITLES):
            return True
    low = head.lower()
    return any(tok in low for tok in _CHALLENGE_TOKENS)


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
                # A challenge page arrives as HTTP 200 + HTML. Treat it as a block
                # so it is retried / IP-rotated, instead of being handed to a
                # parser that will quietly extract nothing from it.
                if is_block_page(resp.text):
                    raise BlockedError("challenge/interstitial page (HTTP 200)")
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

    def fetch_courses_list(self, college_id: Any, page: int) -> Dict[str, Any]:
        """Fetch one page of a college's course list from the internal
        pagination API. Same retry / proxy-rotation / adaptive handling as
        fetch(). Non-JSON responses and JSON sentinels {"status": 301|404} are
        treated as soft failures (retried, then raised for the caller to mark
        the college 'partial')."""
        params = {"data": courses_list_payload(college_id, page)}
        headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            proxy = self.pm.get(self.session_id)
            try:
                resp = self.session.get(
                    COURSES_LIST_API, params=params, headers=headers,
                    proxies=proxy.as_dict() if proxy else None, timeout=self.timeout)
                self.stats.add(requests=1, byts=len(resp.content or b""))
                if resp.status_code in (403, 429, 503):
                    raise BlockedError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "")
                if "json" not in ctype and not resp.text.lstrip().startswith("{"):
                    raise BlockedError("non-JSON courses-list response")
                data = resp.json()
                if data.get("status") in (301, 404):
                    raise BlockedError(f"courses-list status {data.get('status')}")
                self.pm.report_success(proxy)
                if self.adaptive:
                    self.adaptive.on_success()
                if self.verbose:
                    self.log(f"   · courses-list col {college_id} p{page} → "
                             f"{len(data.get('courses') or [])} groups "
                             f"(hasNext={data.get('hasNext')})")
                return data
            except (BlockedError, requests.RequestException, ValueError) as err:
                last_err = err
                self.stats.add(blocks=1)
                self.pm.report_failure(proxy)
                rotated = self._maybe_rotate(err)
                if self.adaptive and not rotated:
                    self.adaptive.on_block()
                wait = self.backoff * attempt + random.uniform(0, 2)
                self.log(f"  ! courses-list col {college_id} p{page} attempt "
                         f"{attempt}/{self.max_retries} failed: {err}"
                         f"{' [rotating IP]' if rotated else ''} -> retry in {wait:.0f}s")
                time.sleep(wait)
        raise RuntimeError(f"courses-list failed after {self.max_retries} attempts: {last_err}")

    def fetch_listing(self, slug: str, page: int) -> Dict[str, Any]:
        """Fetch one page of the india-colleges directory listing API. Returns the
        JSON dict as-is — it may contain 'colleges' (normal states) or a
        'nearby_city_page' shape (tiny states, no 'colleges' key). Same retry /
        proxy-rotation / adaptive handling as fetch()."""
        params = {"data": listing_payload(slug, page)}
        headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            proxy = self.pm.get(self.session_id)
            try:
                resp = self.session.get(
                    LISTING_API, params=params, headers=headers,
                    proxies=proxy.as_dict() if proxy else None, timeout=self.timeout)
                self.stats.add(requests=1, byts=len(resp.content or b""))
                if resp.status_code in (403, 429, 503):
                    raise BlockedError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                ctype = resp.headers.get("Content-Type", "")
                if "json" not in ctype and not resp.text.lstrip().startswith("{"):
                    raise BlockedError("non-JSON listing response")
                data = resp.json()
                if data.get("status") in (301, 404):
                    raise BlockedError(f"listing status {data.get('status')}")
                self.pm.report_success(proxy)
                if self.adaptive:
                    self.adaptive.on_success()
                if self.verbose:
                    self.log(f"   · listing {slug} p{page} → "
                             f"{len(data.get('colleges') or [])} colleges "
                             f"(hasNext={data.get('hasNext')})")
                return data
            except (BlockedError, requests.RequestException, ValueError) as err:
                last_err = err
                self.stats.add(blocks=1)
                self.pm.report_failure(proxy)
                rotated = self._maybe_rotate(err)
                if self.adaptive and not rotated:
                    self.adaptive.on_block()
                wait = self.backoff * attempt + random.uniform(0, 2)
                self.log(f"  ! listing {slug} p{page} attempt {attempt}/{self.max_retries} "
                         f"failed: {err}{' [rotating IP]' if rotated else ''} -> retry in {wait:.0f}s")
                time.sleep(wait)
        raise RuntimeError(f"listing failed after {self.max_retries} attempts: {last_err}")


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
    "colleges_directory": "upsert_colleges_directory",
}


def _write_rows(job_id: int, cfg: Dict[str, Any], table: str,
                rows: List[Dict[str, Any]], db_path: str) -> int:
    """Route a runner's output: to per-job staging (default) or straight to
    master (fallback when cfg['staging'] is False)."""
    if not rows:
        return 0
    # Enrichment B — backfill mode patches only empty columns on existing course
    # rows, writing straight to master (no staging/QC gate: it never adds rows,
    # only tops up gaps in ones already scraped).
    if table == "courses" and cfg.get("backfill_only"):
        return db.upsert_courses(rows, db_path=db_path, fill_empty=True)
    if cfg.get("staging", True):
        return db.stage_records(job_id, table, rows, db_path=db_path)
    return getattr(db, _UPSERT_MAP[table])(rows, db_path=db_path)


def _staged_or_master_count(job_id: int, cfg: Dict[str, Any], db_path: str) -> int:
    if cfg.get("staging", True):
        return sum(db.staged_summary(job_id, db_path=db_path).values())
    return sum(db.counts(db_path=db_path).values())


def _written_count(job_id: int, cfg: Dict[str, Any], table: str, db_path: str) -> int:
    """Rows this job has produced, whether they are still in staging or have
    already been promoted. Counting staging alone breaks the moment incremental
    promotion empties it — the progress counter would fall back to 0 mid-run."""
    if not cfg.get("staging", True):
        return db.counts(db_path=db_path).get(table, 0)
    return (db.count_promoted(job_id, table, db_path=db_path)
            + db.staged_summary(job_id, db_path=db_path).get(table, 0))


def _maybe_flush(job_id: int, cfg: Dict[str, Any], db_path: str) -> None:
    """Incremental promotion: move staged rows to master mid-run (in memory-safe
    chunks) so the data survives an interrupted/OOM-killed job and staging stays
    small. Enabled by default; turn off cfg['incremental_promote'] for the strict
    stage-all-then-validate-then-promote gate."""
    if cfg.get("staging", True) and cfg.get("incremental_promote", True):
        try:
            db.flush_job_staging(job_id, db_path=db_path)
        except Exception:  # noqa: BLE001
            pass


def _finalize_job(job_id: int, cfg: Dict[str, Any], log: Callable[[str], None],
                  base_msg: str, db_path: str) -> None:
    """Finalize a completed job. With incremental promotion on (default), the bulk
    is already in master — just flush the remaining tail and mark promoted. In the
    strict mode, validate the full staged set and auto-promote only if it passes."""
    if not cfg.get("staging", True):
        db.update_job(job_id, status="completed", message=base_msg,
                      finished_at=time.time(), db_path=db_path)
        return
    incremental = cfg.get("incremental_promote", True)
    v = db.validate_job(job_id, cfg.get("validation_rules") or {}, db_path=db_path)
    staged = sum(db.staged_summary(job_id, db_path=db_path).values())
    db.update_job(job_id, quality_score=v["score"], staged_rows=staged, db_path=db_path)
    auto = cfg.get("auto_promote", True)
    if incremental or (v["passed"] and auto):
        summ = db.flush_job_staging(job_id, db_path=db_path)
        db.update_job(job_id, promote_status="promoted", db_path=db_path)
        tag = ("promoted (incremental, memory-safe)" if incremental
               else f"QC {v['score']:.0f}/100 ✓ promoted ({sum(summ.values())} rows)")
        msg = f"{base_msg} · {tag}"
        db.update_job(job_id, status="completed", message=msg,
                      finished_at=time.time(), db_path=db_path)
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
    delay = float(cfg.get("delay", 1.0))
    # Share Stats/AdaptiveDelay with the client so this phase reports its request
    # count and bandwidth like every other phase (it previously reported 0/0, so
    # the UI's Bandwidth metric and budget forecasts were wrong for Phase 1).
    stats = Stats()
    adaptive = AdaptiveDelay(delay, enabled=bool(cfg.get("adaptive", True)))
    client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                    backoff=float(cfg.get("backoff", 4)), stats=stats, adaptive=adaptive)
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
        written = _written_count(job_id, cfg, "courses", db_path)
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
            # Promote staged rows to master as we go (as every other phase does).
            # Without this, Phase-1 data stays in staging for the whole job, so
            # run_pipeline's Phase-2 leg reads an empty/stale master courses table
            # — and an interrupted Phase-1 job leaves everything unpromoted.
            _maybe_flush(job_id, cfg, db_path)
            written = _written_count(job_id, cfg, "courses", db_path)
            pages_done += 1
            page += 1
            reqs, byts, _ = stats.snapshot()
            db.set_setting("courses_resume_page", page, db_path=db_path)
            db.update_job(job_id, done_units=written, items_written=written,
                          req_count=reqs, bytes_count=byts,
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
        _maybe_flush(job_id, cfg, db_path)   # incremental promote (see run_courses)
        written = _written_count(job_id, cfg, "courses", db_path)
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
        written = _written_count(job_id, cfg, "courses", db_path)
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
    # Phase 2 selects its work from the MASTER courses table (db.list_course_ids),
    # so every course Phase 1 just scraped must be promoted out of staging before
    # it starts — otherwise Phase 2 runs against a stale/empty list and reports
    # "0 courses, 0 offerings" while looking like a success.
    if cfg.get("staging", True):
        try:
            promoted = db.flush_job_staging(job_id, db_path=db_path)
            if promoted:
                log("  promoted phase-1 staging → master: "
                    + ", ".join(f"{k}={v:,}" for k, v in promoted.items()))
        except Exception as err:  # noqa: BLE001
            log(f"  ! could not promote phase-1 staging before phase 2: {err}")
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
            _maybe_flush(job_id, cfg, db_path)   # incremental promote (memory-safe)

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
                # get_text now raises BlockedError on a challenge page, so a
                # blocked college never reaches the write below and keeps
                # enriched_at NULL — i.e. it stays in the queue for a later run.
                # Previously it was stamped 'enriched' with all-NULL contacts and
                # was never retried again.
                html = client.get_text(cobj["link"])
                fields = parse_college_ld(html)
                with db_lock:
                    # A page that fetched cleanly but genuinely has no JSON-LD is
                    # still marked done (fields == {}), so it isn't re-fetched
                    # forever. update_college_details never overwrites an existing
                    # value with a blank one.
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
            return {"college_name": college_name,
                    "courses": _course_group_rows(clist, hostel),
                    "groups": clist, "hostel": hostel,
                    "course_count": cd.get("course_count"),
                    "total_pages": cd.get("total_pages")}
    tbl = _parse_cf_tables(page_html)
    tbl.setdefault("groups", [])
    # The hostel fee is only ever in the HTML tables, so it must be read on this
    # path too — the fallback previously returned "" and silently dropped it,
    # i.e. exactly when the __NEXT_DATA__ JSON was missing and we needed it most.
    tbl["hostel"] = tbl.get("hostel") or _hostel_from_tables(page_html)
    tbl.setdefault("total_pages", 1)
    return tbl


_URL_KEYS = ("url", "course_url", "course_link", "seo_url", "landing_url",
             "landing_page", "page_url", "link", "slug", "course_slug", "seo_slug")


def _first_url(*objs: Dict[str, Any]) -> str:
    """Best-effort per-course URL from a course/stream object. The courses-list
    API's exact URL key isn't documented, so we probe the common candidates (most
    specific object first) and absolutise whatever we find. Empty if none — the
    caller then falls back to the college's courses-fees page."""
    for o in objs:
        if not isinstance(o, dict):
            continue
        for k in _URL_KEYS:
            v = o.get(k)
            if isinstance(v, str) and v.strip():
                return abs_url(v.strip())
    return ""


def _course_group_rows(clist: List[Dict[str, Any]], hostel: str = "",
                       seen: Optional[set] = None) -> List[Dict[str, Any]]:
    """Flatten a course_data.courses[] list (from __NEXT_DATA__ or the
    courses-list API — identical schema) into per-(course, specialization) rows.
    Pass a shared `seen` set to dedupe rows across multiple pages of one college."""
    if seen is None:
        seen = set()
    rows: List[Dict[str, Any]] = []
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
            rows.append({
                "course_name": full,
                "specialization": spec if is_spec else "",
                "course_url": _first_url(s, c),
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
    return rows


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
        # Known Phase-2 colleges + any colleges queued from the Directory gap.
        ids = sorted(set(db.list_known_college_ids(db_path=db_path))
                     | set(db.list_cc_queued_ids(db_path=db_path)))
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
            _maybe_flush(job_id, cfg, db_path)   # incremental promote (memory-safe)

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
            client.session_id = f"col{cid}"          # one sticky IP: page-1 HTML + all API pages
            url = f"{SITE}/college/{cid}/courses-fees"
            prog = db.get_cc_progress(cid, db_path=db_path) or {}
            resume_from = int(prog.get("last_page") or 0)
            n = int(prog.get("found") or 0) if resume_from else 0
            hostel = ""
            seen_rows: set = set()               # row-level dedup across pages
            seen_groups: set = set()             # group-level dedup on (name/slug, program type)

            def _dedupe_groups(groups):
                fresh = []
                for g in (groups or []):
                    gk = ((g.get("short_head") or g.get("display_name") or "").strip().lower(),
                          (g.get("course_type") or "").strip().lower())
                    if not gk[0] or gk in seen_groups:
                        continue
                    seen_groups.add(gk)
                    fresh.append(g)
                return fresh

            def _stage(groups, cname):
                nonlocal n
                rows = _course_group_rows(_dedupe_groups(groups), hostel, seen_rows)
                rows = [{**r, "college_id": cid, "college_name": cname,
                         "source_url": url,
                         "course_url": r.get("course_url") or url,
                         "scraped_at": time.time()} for r in rows]
                if rows:
                    with db_lock:
                        n += _write_rows(job_id, cfg, "college_courses", rows, db_path)

            try:
                # College name from already-scraped tables (avoids the heavy HTML).
                cname = db.college_name_lookup(cid, db_path=db_path)
                # Hostel fee lives only on the ~600 KB SSR page — fetch it ONLY if
                # asked (off by default: that page is slow and 403-prone, and it's
                # what was ballooning bandwidth/ETA).
                if cfg.get("fetch_hostel", False):
                    try:
                        _p = parse_courses_fees(client.get_text(url))
                        hostel = _p.get("hostel", "")
                        cname = cname or _p.get("college_name", "")
                    except Exception:  # noqa: BLE001
                        pass
                # Page 1 + total_pages come from the small JSON courses-list API
                # (not the HTML) — ~12 KB vs ~600 KB, and far fewer 403s.
                first = client.fetch_courses_list(cid, 1)
                total_pages = int(first.get("total_pages") or 1)
                if resume_from:
                    grp1 = _dedupe_groups(first.get("courses") or [])
                    _course_group_rows(grp1, hostel, seen_rows)   # seed dedupe sets
                    start = resume_from + 1
                else:
                    _stage(first.get("courses") or [], cname)
                    with db_lock:
                        db.set_cc_progress(cid, "partial", n, last_page=1, db_path=db_path)
                    start = 2
                # Pages 2..total_pages via the internal courses-list API.
                completed = True
                if total_pages > 1 and start <= total_pages:
                    for page, data in iter_course_pages(
                            lambda p: client.fetch_courses_list(cid, p),
                            total_pages, start_page=start):
                        _stage(data.get("courses") or [], cname)
                        with db_lock:
                            db.set_cc_progress(cid, "partial", n, last_page=page, db_path=db_path)
                        if db.stop_requested(job_id, db_path=db_path) or budget_hit():
                            completed = False
                            break
                        time.sleep(adaptive.value())
                if completed:
                    with db_lock:
                        db.set_cc_progress(cid, "done" if n else "empty", n,
                                           last_page=total_pages, db_path=db_path)
            except Exception as err:  # noqa: BLE001
                # Soft failure (non-JSON / api 301|404 / proxy): leave the college
                # 'partial' at the last good page so a resume retries the rest.
                with db_lock:
                    db.set_cc_progress(cid, "partial", n, last_page=resume_from, db_path=db_path)
                log(f"  college {cid} soft-fail: {str(err)[:70]}")
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


# ---------------------------------------------------------------------------
# Directory phase: the complete india-colleges directory (coverage baseline)
# ---------------------------------------------------------------------------
def _nextdata_pageprops(page_html: str) -> Dict[str, Any]:
    """Return the __NEXT_DATA__ pageProps from a listing page. Raw server HTML
    nests it under props.initialProps.pageProps; hydrated DOM uses props.pageProps."""
    m = _NEXTDATA_RE.search(page_html or "")
    if not m:
        return {}
    try:
        nd = json.loads(m.group(1))
    except Exception:  # noqa: BLE001
        return {}
    props = nd.get("props") or {}
    ip = (props.get("initialProps") or {}).get("pageProps")
    return ip or props.get("pageProps") or {}


def _flatten_colleges(x: Any, out: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """Collect every college-like dict (has college_id + college_name) from an
    arbitrarily-nested structure — the tiny-state HTML nests colleges 2 levels deep."""
    if out is None:
        out = []
    if isinstance(x, dict):
        if "college_id" in x and "college_name" in x:
            out.append(x)
        else:
            for v in x.values():
                _flatten_colleges(v, out)
    elif isinstance(x, list):
        for v in x:
            _flatten_colleges(v, out)
    return out


DIR_IMG_BASE = "https://image-static.collegedunia.com/"


def _as_dict(v: Any) -> Dict[str, Any]:
    """This API uses [] to mean 'absent' and a dict to mean 'present' for several
    keys (placement, reviewsData, sa reviews). Normalise both to a dict."""
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        for x in v:
            if isinstance(x, dict):
                return x
    return {}


def _img(path: Any) -> str:
    p = str(path or "").strip()
    if not p:
        return ""
    return p if p.startswith("http") else DIR_IMG_BASE + p.lstrip("/")


def parse_directory_extras(c: Dict[str, Any]) -> Dict[str, Any]:
    """Fields the india-colleges listing already returns on every row but which
    nothing captured — placement, facilities, review aggregates, media, and the
    availableTabs map that says which sub-pages exist for this college.

    All of it is already inside raw_json for previously-scraped rows, so this can
    be replayed offline with no further requests (see reparse.py)."""
    # `fees` arrives in several shapes: the listing API sends a list of dicts
    # keyed 'fee'/'fee_formatted' (the original parser probed 'fees'/'amount'/
    # 'value'/'total_fees' and so returned "" on every row), while the tiny-state
    # HTML fallback can send a plain list of strings. Handle all of them.
    fees = c.get("fees")
    if isinstance(fees, dict):
        fees = [fees]
    first = fees[0] if isinstance(fees, list) and fees else None
    top: Dict[str, Any] = first if isinstance(first, dict) else {}
    if isinstance(first, str):
        top_fee_display = first.strip()          # already formatted upstream
    elif top:
        fmt = str(top.get("fee_formatted") or top.get("fees") or top.get("amount")
                  or top.get("value") or top.get("total_fees") or "").strip()
        top_fee_display = fmt if (not fmt or fmt.startswith("₹")) else "₹" + fmt
    elif first is not None:
        top_fee_display = str(first).strip()
    else:
        top_fee_display = ""

    pl = _as_dict(c.get("placement"))
    # NOTE: the payload also carries reviewsData — aggregate ratings plus one
    # named student's review text. Reviews are deliberately NOT extracted or
    # stored anywhere; the key is left untouched in raw_json.
    tabs = c.get("availableTabs")
    tab_keys = sorted(tabs.keys()) if isinstance(tabs, dict) else []
    fac = c.get("facilities")
    fac_list = [str(f) for f in fac if f] if isinstance(fac, list) else []
    sr = _as_dict(c.get("stream_ranking"))

    return {
        "top_course_fees": top_fee_display,
        "top_course_name": str(top.get("name") or top.get("short_form") or ""),
        "top_course_id": str(top.get("course_id") or ""),
        "top_course_fee_inr": _to_int(top.get("fee")) if top else None,
        "top_course_link": abs_url(top.get("link")) if top.get("link") else "",
        "courses_fees_json": json.dumps(fees, ensure_ascii=False) if fees else "",
        "placement_avg_salary": _to_int(pl.get("average_salary")),
        "placement_highest_salary": _to_int(pl.get("highest_salary")),
        "placement_percentage": _to_float(c.get("placement_percentage")),
        "facilities": ", ".join(fac_list),
        "facilities_count": len(fac_list),
        "major_stream_rating": _to_float(c.get("major_stream_rating")),
        "stream_ranking_count": _to_int(sr.get("count")),
        "available_tabs": ", ".join(tab_keys),
        "has_scholarship_page": 1 if "scholarship" in tab_keys else 0,
        "has_placement_page": 1 if "placement" in tab_keys else 0,
        "has_ranking_page": 1 if "ranking" in tab_keys else 0,
        "has_faculty_page": 1 if "faculty" in tab_keys else 0,
        "has_hostel_page": 1 if "hostel" in tab_keys else 0,
        "has_news_page": 1 if "news" in tab_keys else 0,
        "has_admission_page": 1 if "admission" in tab_keys else 0,
        "tagline": str(c.get("tagline") or ""),
        "listing_name": str(c.get("listing_name") or ""),
        "logo_url": _img(c.get("logo")),
        "cover_url": _img(c.get("cover")),
        "photo_count": _to_int(c.get("photoCount")),
        "video_count": _to_int(c.get("videoCount")),
        "cutoff_url": abs_url(c.get("view_all_course")) if c.get("view_all_course") else "",
    }


def parse_directory_rankings(c: Dict[str, Any]) -> List[Dict[str, Any]]:
    """rankingData[] -> one row per (agency, year, stream). Real ranking records
    (NIRF / India Today / Collegedunia ...) that were previously discarded."""
    cid = _to_int(c.get("college_id"))
    out: List[Dict[str, Any]] = []
    if cid is None:
        return out
    for r in (c.get("rankingData") or []):
        if not isinstance(r, dict):
            continue
        agency = str(r.get("agency") or "").strip()
        year = _to_int(r.get("year"))
        if not agency and year is None:
            continue
        out.append({
            "college_id": cid,
            "agency": agency,
            "agency_id": _to_int(r.get("agencyId")),
            "year": year,
            "stream": str(r.get("stream") or ""),
            "rank": _to_int(r.get("rankingOfCollege")),
            "out_of": _to_int(r.get("rankingOutOfTotalNoOfCollege")),
            "category_ranking": str(r.get("category_ranking") or ""),
            "logo": _img(r.get("logo")) if r.get("logo") else "",
            "scraped_at": time.time(),
        })
    return out


def parse_directory_college(c: Dict[str, Any], source_slug: str = "") -> Dict[str, Any]:
    """Flatten one directory college object (identical shape from the listing API
    and the tiny-state HTML) into a colleges_directory row."""
    approvals = c.get("approvals")
    if isinstance(approvals, list):
        approvals = ", ".join(
            str(a.get("name") if isinstance(a, dict) else a) for a in approvals if a)
    elif not isinstance(approvals, str):
        approvals = ""
    row = {
        "college_id": _to_int(c.get("college_id")),
        "name": c.get("college_name", "") or "",
        "short_form": c.get("college_short_form", "") or "",
        "city": c.get("college_city", "") or "",
        "city_id": _to_int(c.get("city_id")),
        "state": c.get("state", "") or "",
        "state_id": _to_int(c.get("state_id")),
        "link": abs_url(c.get("url")),
        "rating": _to_float(c.get("rating")),
        "naac_grading": c.get("naac_grading", "") or "",
        "course_count": _to_int(c.get("courseCount")),
        "approvals": approvals,
        "source_slug": source_slug,
        "raw_json": json.dumps(c, ensure_ascii=False),
        "scraped_at": time.time(),
    }
    row.update(parse_directory_extras(c))   # incl. the fixed top_course_fees
    return row


def fetch_state_filters(client: "Client") -> List[Dict[str, Any]]:
    """Fetch india-colleges once and return the state partitions:
    [{text, state_id, count, slug}] (slug is the last path segment of link)."""
    html = client.get_text(f"{SITE}/india-colleges")
    pp = _nextdata_pageprops(html)
    vals = ((((pp.get("filterResponse") or {}).get("filters") or {}).get("state")
             or {}).get("values")) or []
    states = []
    for s in vals:
        link = (s.get("link") or "").rstrip("/")
        slug = link.split("/")[-1] if link else ""
        if not slug:
            continue
        states.append({"text": s.get("text", ""), "state_id": _to_int(s.get("value")),
                       "count": _to_int(s.get("count")) or 0, "slug": slug})
    return states


def parse_listing_html_colleges(page_html: str):
    """Tiny-state fallback: pull colleges from the HTML listing page's
    __NEXT_DATA__ (listingResponse.colleges is nested; flatten it)."""
    pp = _nextdata_pageprops(page_html)
    lr = pp.get("listingResponse") or {}
    return _flatten_colleges(lr.get("colleges")), _to_int(lr.get("count"))


def run_directory(job_id: int, cfg: Dict[str, Any], db_path: str = db.DB_PATH,
                  log: Optional[Callable[[str], None]] = None) -> None:
    """Phase 'Directory': scrape the full india-colleges directory as a coverage
    baseline. Partitions by state (defeats the ~999-page listing ceiling), with a
    tiny-state HTML fallback and an optional india-colleges base sweep. Rows go
    through staging → validate → promote; quality rule requires the promoted count
    to be within 5% of the API-reported total."""
    log = log or (lambda m: print(m, flush=True))
    pm = ProxyManager.from_config(cfg)
    stats = Stats()
    adaptive = AdaptiveDelay(float(cfg.get("delay", 1.0)), enabled=bool(cfg.get("adaptive", True)))
    budget_bytes = int(float(cfg.get("budget_mb", 0)) * 1024 * 1024)
    budget_requests = int(cfg.get("budget_requests", 0))
    base_sweep = bool(cfg.get("base_sweep", True))
    client = Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                    backoff=float(cfg.get("backoff", 4)), stats=stats, adaptive=adaptive)

    def budget_hit():
        reqs, byts, _ = stats.snapshot()
        if budget_requests and reqs >= budget_requests:
            return f"request budget reached ({reqs})"
        if budget_bytes and byts >= budget_bytes:
            return f"bandwidth budget reached ({byts/1048576:.1f} MB)"
        return None

    db.update_job(job_id, status="running", message="discovering states…", db_path=db_path)
    log("Directory phase [BUILD: dir-v1] — fetching india-colleges state filters.")
    client.session_id = "dirstates"
    try:
        states = fetch_state_filters(client)
    except Exception as err:  # noqa: BLE001
        states = []
        log(f"  ! state filter fetch failed: {err}")
    expected = sum(s.get("count") or 0 for s in states) if states else 20695
    try:
        db.set_setting("dir_states", states, db_path=db_path)
    except Exception:  # noqa: BLE001
        pass
    log(f"Discovered {len(states)} states; expected ≈ {expected:,} colleges.")

    partitions: List = []
    if base_sweep:
        partitions.append(("india-colleges", DIR_BASE_SWEEP_CAP))
    partitions += [(s["slug"], None) for s in states if s.get("slug")]
    total_units = len(partitions)
    db.update_job(job_id, total_units=total_units, db_path=db_path)

    done_slugs = set() if cfg.get("force_rescrape") else db.get_dir_done_slugs(db_path=db_path)
    processed = 0
    incomplete = False
    try:
        for slug, page_cap in partitions:
            if db.stop_requested(job_id, db_path=db_path):
                incomplete = True
                break
            bh = budget_hit()
            if bh:
                incomplete = True
                log(f"  {bh} — stopping.")
                break
            if slug in done_slugs:
                processed += 1
                continue
            client.session_id = f"dir{abs(hash(slug)) % (10 ** 8)}"   # sticky IP per partition
            prog = db.get_dir_progress(slug, db_path=db_path) or {}
            found = int(prog.get("found") or 0)
            start = int(prog.get("last_page") or 0) + 1
            cap = int(page_cap or MAX_DIR_SLUG_PAGES)
            partition_ok = True
            try:
                page = start
                while page <= cap:
                    if db.stop_requested(job_id, db_path=db_path) or budget_hit():
                        partition_ok = False
                        incomplete = True
                        break
                    data = client.fetch_listing(slug, page)
                    colls = data.get("colleges")
                    if colls is None:
                        # nearby_city_page (tiny state) -> HTML listing fallback
                        cobjs, _cnt = parse_listing_html_colleges(client.get_text(f"{SITE}/{slug}"))
                        rows = [parse_directory_college(c, slug) for c in cobjs]
                        found += _write_rows(job_id, cfg, "colleges_directory", rows, db_path)
                        db.set_dir_progress(slug, "done", found, page, db_path=db_path)
                        break
                    if not colls:
                        break   # empty page = genuine end / past the ceiling
                    rows = [parse_directory_college(c, slug) for c in colls]
                    found += _write_rows(job_id, cfg, "colleges_directory", rows, db_path)
                    db.set_dir_progress(slug, "partial", found, page, db_path=db_path)
                    if page % 10 == 0:
                        # Live metrics + incremental flush mid-partition (the base
                        # sweep is one long 998-page partition, so don't wait for it
                        # to finish before updating the dashboard / promoting).
                        _maybe_flush(job_id, cfg, db_path)
                        reqs, byts, _ = stats.snapshot()
                        got = (db.count_promoted(job_id, "colleges_directory", db_path=db_path)
                               + db.staged_summary(job_id, db_path=db_path).get("colleges_directory", 0))
                        db.update_job(job_id, items_written=got, req_count=reqs, bytes_count=byts,
                                      message=f"{slug} p{page} · {got:,} colleges · "
                                              f"{byts/1048576:.1f} MB", db_path=db_path)
                    if not data.get("hasNext", False):
                        break
                    page += 1
                    time.sleep(adaptive.value())
                if partition_ok:
                    db.set_dir_progress(slug, "done", found, page, db_path=db_path)
            except Exception as err:  # noqa: BLE001
                db.set_dir_progress(slug, "partial", found,
                                    int(prog.get("last_page") or 0), db_path=db_path)
                log(f"  ! partition '{slug}' soft-fail: {str(err)[:70]}")
            _maybe_flush(job_id, cfg, db_path)   # incremental promote (memory-safe)
            processed += 1
            reqs, byts, _ = stats.snapshot()
            got = ((db.count_promoted(job_id, "colleges_directory", db_path=db_path)
                    + db.staged_summary(job_id, db_path=db_path).get("colleges_directory", 0))
                   if cfg.get("staging", True) else db.counts(db_path=db_path).get("colleges", 0))
            db.update_job(job_id, done_units=processed, items_written=got,
                          req_count=reqs, bytes_count=byts,
                          message=f"{processed}/{total_units} partitions · {got:,} colleges · "
                                  f"{byts/1048576:.1f} MB", db_path=db_path)
            log(f"  ✓ {slug}: +{found} colleges ({processed}/{total_units} partitions)")
    except Exception as err:  # noqa: BLE001
        db.update_job(job_id, status="error", message=str(err)[:300],
                      finished_at=time.time(), db_path=db_path)
        log(f"ERROR: {err}")
        raise

    # Quality gate: promoted rows must be within 5% of the API-reported total.
    cfg.setdefault("validation_rules", {})
    cfg["validation_rules"]["min_rows"] = int(0.95 * expected) if expected else 1
    _maybe_flush(job_id, cfg, db_path)
    reqs, byts, _ = stats.snapshot()
    got = ((db.count_promoted(job_id, "colleges_directory", db_path=db_path)
            + db.staged_summary(job_id, db_path=db_path).get("colleges_directory", 0))
           if cfg.get("staging", True) else db.counts(db_path=db_path).get("colleges", 0))
    if incomplete or db.stop_requested(job_id, db_path=db_path):
        msg = (f"INCOMPLETE — {processed}/{total_units} partitions, {got:,} directory "
               f"colleges. Resume to continue.")
        db.update_job(job_id, status="stopped", message=msg, finished_at=time.time(),
                      req_count=reqs, bytes_count=byts, db_path=db_path)
        log(msg)
    else:
        msg = (f"done: {got:,}/{expected:,} directory colleges across "
               f"{total_units} partitions, {byts/1048576:.1f} MB")
        db.update_job(job_id, status="completed", message=msg, finished_at=time.time(),
                      req_count=reqs, bytes_count=byts, db_path=db_path)
        log(msg)
        send_notification(cfg, "Collegedunia Directory complete", msg, log)
