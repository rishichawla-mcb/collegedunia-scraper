"""
Shiksha — scraper. Self-contained: writes ONLY sk_ tables, in their OWN db file.

Phase Ⓐ  discovery
------------------
Shiksha publishes its entire inventory in sitemaps, so discovery is ~48 gzipped
requests rather than a paginated crawl. Measured 2026-09-24 across all 28 college
sitemaps:

    669,514 college URLs · 84,193 "college home" URLs · 57,695 DISTINCT ids

Three facts shape this phase:

1. **The id is in the URL** — `/college/<slug>-<id>`. Every college id is known
   before a single college page is fetched, unlike Collegedunia where ids only
   arrived inside listing payloads.
2. **Dedupe by id, not by URL.** 84,193 home URLs resolve to 57,695 ids: ~26k
   alias slugs. Crawling by URL would make phase B ~46% larger for nothing. The
   aliases are still stored (`sk_college_aliases`) — nothing discovered is
   discarded.
3. **The offering edges are free.** `/college/<slug>-<id>/course-<cslug>-<courseId>`
   carries BOTH ids, so every college x course edge comes out of the sitemap at
   no request cost — the equivalent of Collegedunia's Phase B, for nothing.

Proxy
-----
Owner instruction, 2026-09-24: *"also use proxy always"*. This module REFUSES to
run without one. Shiksha has never been touched from a datacentre IP (all recon
came from the owner's residential browser), so the first run is also the test of
whether it blocks proxies the way Collegedunia does — and a direct run would put
the Render egress IP, not a disposable one, in front of that question.

Reuses the shared HTTP client from `scraper`: proxy rotation, block detection,
exponential backoff, Retry-After and wire-byte accounting all come along.
"""
from __future__ import annotations

BUILD = "2026-09-24a"

import gzip
import io
import os
import queue as _queue
import re
import threading
import time
import zlib
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests

import db as _core
import sk_db
from scraper import (AdaptiveDelay, BlockedError, Client, ProxyManager, Stats,
                     is_block_page)

SITE = "https://www.shiksha.com"
SITEMAP_INDEX = f"{SITE}/sitemap_index.xml"

CHROME_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def sk_headers() -> Dict[str, str]:
    """Headers for shiksha.com. NOT `scraper.base_headers()`.

    The first discovery run used `base_headers()` and was refused five times
    over. Those headers were written for Collegedunia's web-api and send
    `Referer: https://collegedunia.com/course-finder`, `X-Requested-With:
    XMLHttpRequest` and `Accept: application/json` — a cross-site referer and an
    AJAX marker on a request for a static XML file. That combination is bot-like
    enough that the 403 said nothing about whether the IP was acceptable; it was
    my bug, not a finding about Shiksha. See sk_probe.py, which measures which
    of the two it actually was.

    `Accept-Encoding` deliberately omits `br` and `zstd`: brotli is not installed
    here, and advertising an encoding we cannot decode turns a good response into
    an unreadable one.
    """
    return {
        "User-Agent": CHROME_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-IN,en-GB;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", '
                     '"Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Connection": "keep-alive",
    }

# The sitemap families worth reading. `www_news_SiteMap` (137 files) and the
# category/exam/ranking/review families are deliberately NOT here: they describe
# editorial pages, not the college inventory. Overridable via cfg["sitemap_groups"].
SITEMAP_GROUPS = ("www_sd_college_SiteMap", "www_sd_university_SiteMap",
                  "www_listing_SiteMap")

# Tabs a college page publishes. Anything else under /college/<slug>-<id>/ that
# is not an offering is recorded as 'other' rather than silently dropped.
COLLEGE_TABS = ("courses", "fees", "placement", "reviews", "admission",
                "cutoff", "gallery", "questions")

FLUSH_EVERY = 5000          # rows held before a batch is written


# ---------------------------------------------------------------------------
# URL classification
# ---------------------------------------------------------------------------
# The slug is `[^/]+?` and NOT `.+?`: `.` matches a slash, so `.+?` let the home
# pattern swallow a whole offering URL —
# '/college/iim-ahmedabad-72/course-btech-202' came back as a college home with
# slug 'iim-ahmedabad-72/course-btech' and id 202, silently inventing a college
# per offering and losing every edge. Caught by the classify() tests, 2026-09-24.
#
# Non-greedy slug plus an anchored trailing -(\d+) means the id is always the
# LAST numeric segment of that one path segment: '/college/…-jaipur-72' -> slug
# '…-jaipur', id 72, and '/college/nit-trichy-2-99' -> id 99, not 2.
_COLLEGE_HOME = re.compile(r"^/college/(?P<slug>[^/]+?)-(?P<id>\d+)/?$", re.I)
_COLLEGE_SUB = re.compile(r"^/college/(?P<slug>[^/]+?)-(?P<id>\d+)/(?P<rest>.+?)/?$", re.I)
_UNIV_HOME = re.compile(r"^/university/(?P<slug>[^/]+?)-(?P<id>\d+)/?$", re.I)
_UNIV_SUB = re.compile(r"^/university/(?P<slug>[^/]+?)-(?P<id>\d+)/(?P<rest>.+?)/?$", re.I)
_OFFERING = re.compile(r"^course-(?P<cslug>[^/]+?)-(?P<cid>\d+)$", re.I)


def _to_int(v) -> Optional[int]:
    try:
        return int(str(v).strip())
    except Exception:  # noqa: BLE001
        return None


def classify(url: str) -> Dict[str, Any]:
    """One sitemap <loc> -> what it is. Returns {'kind': ...} plus ids.

    kinds: 'college_home' | 'college_tab' | 'offering' | 'university_home' |
           'university_sub' | 'other'
    """
    try:
        path = urlparse(url).path or ""
    except Exception:  # noqa: BLE001
        return {"kind": "other"}
    path = path.rstrip("/") or "/"

    m = _COLLEGE_HOME.match(path)
    if m:
        return {"kind": "college_home", "college_id": _to_int(m.group("id")),
                "slug": m.group("slug")}

    m = _COLLEGE_SUB.match(path)
    if m:
        cid, slug, rest = _to_int(m.group("id")), m.group("slug"), m.group("rest")
        off = _OFFERING.match(rest)
        if off:
            return {"kind": "offering", "college_id": cid, "slug": slug,
                    "course_id": _to_int(off.group("cid")),
                    "course_slug": off.group("cslug")}
        tab = rest.split("/")[0].lower()
        return {"kind": "college_tab", "college_id": cid, "slug": slug,
                "tab": tab if tab in COLLEGE_TABS else "other"}

    m = _UNIV_HOME.match(path)
    if m:
        return {"kind": "university_home", "university_id": _to_int(m.group("id")),
                "slug": m.group("slug")}

    m = _UNIV_SUB.match(path)
    if m:
        return {"kind": "university_sub", "university_id": _to_int(m.group("id")),
                "slug": m.group("slug"), "rest": m.group("rest")}

    return {"kind": "other"}


# ---------------------------------------------------------------------------
# XML parsing — regex, not a DOM parser
# ---------------------------------------------------------------------------
# A sitemap is a flat, machine-generated list; the largest college file is tens
# of MB decompressed. ElementTree would hold the whole tree in memory for every
# one of 48 files, concurrently. Namespaces also differ between Shiksha's index
# and its urlsets, which a namespace-aware parser would have to be told about.
_ENTRY_RE = re.compile(r"<(?:url|sitemap)\b[^>]*>(.*?)</(?:url|sitemap)>", re.S | re.I)
_LOC_RE = re.compile(r"<loc>\s*(.*?)\s*</loc>", re.S | re.I)
_LASTMOD_RE = re.compile(r"<lastmod>\s*(.*?)\s*</lastmod>", re.S | re.I)


def _unescape(s: str) -> str:
    return (s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
             .replace("&quot;", '"').replace("&apos;", "'").strip())


def parse_sitemap(xml: str) -> List[Tuple[str, str]]:
    """[(loc, lastmod)] from a sitemap index OR a urlset. Same shape for both.

    A file truncated mid-transfer has a final <url> block with no closing tag.
    Falling back only when NO block parsed would still silently drop that last
    URL, so the fallback triggers whenever fewer entries were paired than there
    are <loc>s — the locs are the inventory, the lastmods are a bonus."""
    pairs: Dict[str, str] = {}
    out: List[Tuple[str, str]] = []
    for block in _ENTRY_RE.findall(xml or ""):
        loc = _LOC_RE.search(block)
        if not loc:
            continue
        lm = _LASTMOD_RE.search(block)
        u, m = _unescape(loc.group(1)), (_unescape(lm.group(1)) if lm else "")
        out.append((u, m))
        pairs[u] = m
    all_locs = [_unescape(u) for u in _LOC_RE.findall(xml or "")]
    if len(out) < len(all_locs):
        return [(u, pairs.get(u, "")) for u in all_locs]
    return out


def decompress(raw: bytes) -> str:
    """Sitemap bodies are .xml.gz FILES, not gzip Content-Encoding.

    requests transparently undoes Content-Encoding but leaves the file's own
    gzip container intact, so the bytes that arrive still start with 1f 8b. A
    few are served already decoded (some CDNs strip it), so the magic number is
    checked rather than assumed; zlib is the fallback for a deflate wrapper."""
    if not raw:
        return ""
    if raw[:2] == b"\x1f\x8b":
        try:
            return gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode(
                "utf-8", "replace")
        except Exception:  # noqa: BLE001
            pass
        try:
            return zlib.decompress(raw, 16 + zlib.MAX_WBITS).decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            pass
    return raw.decode("utf-8", "replace")


# ---------------------------------------------------------------------------
# The HTTP stack — curl_cffi, not requests
# ---------------------------------------------------------------------------
# Measured 2026-09-25 from the Render host (sk_probe.py, then sk_probe2.py):
#
#   url            client     route      result
#   robots.txt     requests   direct     403
#   robots.txt     curl_cffi  direct     200
#   sitemap_index  requests   direct     403     (and 403 via proxy, and via IN)
#   sitemap_index  curl_cffi  direct     200     (200 via proxy, and via IN)
#
# python-requests is refused by shiksha.com outright — every URL, every header
# profile, including /robots.txt, from this host and through the proxy. The same
# request through curl_cffi impersonating Chrome returns 200 everywhere. So it is
# the TLS ClientHello and HTTP/2 fingerprint, not headers, not the IP, and not
# geography (direct curl_cffi works, so India pinning is not needed).
#
# curl_cffi is used ONLY here. Nothing else in the codebase changes.
try:
    from curl_cffi import requests as _curl        # noqa: N813
    CURL_AVAILABLE = True
except ImportError:                                # pragma: no cover
    _curl = None
    CURL_AVAILABLE = False

# `impersonate="chrome"` tracks the newest Chrome target the installed version
# knows. It replaces the ClientHello, the HTTP/2 SETTINGS and the default header
# ORDER — which is why it is not just another header profile, and why
# `sk_headers()` is NOT passed to it: supplying our own header dict would
# override the impersonated set and put the fingerprint back out of step with
# itself. `sk_headers()` is kept for the probe's comparison rows only.
IMPERSONATE = os.environ.get("CD_SK_IMPERSONATE", "chrome")

# Used only by assert_proxy_effective() — two requests, once per job.
IP_ECHO = "https://api.ipify.org?format=json"


class CurlRequired(RuntimeError):
    """curl_cffi is missing. Shiksha cannot be fetched without it."""


class ProxyIneffective(RuntimeError):
    """A proxy is configured but traffic is not going through it."""


def _session_for(client: Client):
    """One curl_cffi session per Client, created lazily and cached on it.

    A seam on purpose: the test harness replaces this function to install a fake
    transport, which keeps the tests driving the real runner rather than a
    reimplementation of it."""
    s = getattr(client, "_sk_curl", None)
    if s is None:
        if not CURL_AVAILABLE:
            raise CurlRequired(
                "curl_cffi is not installed. shiksha.com refuses python-requests "
                "(403 on every URL, measured 2026-09-25); it is a hard "
                "requirement for this vertical. `pip install curl_cffi`, or "
                "redeploy — it is pinned in requirements.txt.")
        s = _curl.Session(impersonate=IMPERSONATE)
        client._sk_curl = s
    return s


def _proxies(proxy) -> Optional[Dict[str, str]]:
    return {"http": proxy.url, "https": proxy.url} if proxy else None


def _wire(resp) -> int:
    """Bytes off the socket, not the decompressed body.

    curl_cffi has no `raw.tell()`, but it exposes curl's own counters, which are
    better than anything `wire_bytes()` could infer: `download_size` is the
    compressed body as curl received it. Verified locally — 7,411,407 bytes
    downloaded for a 46 MB decompressed response."""
    n = getattr(resp, "download_size", None)
    if n:
        return int(n) + int(getattr(resp, "header_size", 0) or 0)
    cl = resp.headers.get("Content-Length")
    if cl:
        try:
            return int(cl)
        except (TypeError, ValueError):
            pass
    return len(resp.content or b"")


def _transport_error(err: Exception) -> Exception:
    """Translate a curl_cffi transport failure into the requests exception the
    shared retry path understands.

    `Client._classify()` decides whether to rotate the exit IP by checking for
    `requests.exceptions.ConnectionError` / `Timeout`. curl_cffi raises its own
    classes with the same NAMES but a different ancestry, so without this every
    dead tunnel would classify as 'other' and `_rotate()` would not fire — the
    exact failure mode that cost Phase 3 a week in September."""
    if isinstance(err, BlockedError):
        return err
    mod = type(err).__module__.split(".")[0]
    if mod == "curl_cffi":
        return requests.exceptions.ConnectionError(
            f"{type(err).__name__}: {str(err)[:160]}")
    return err


def fetch_bytes(client: Client, url: str, label: str) -> bytes:
    """GET raw bytes through curl_cffi, with the client's own retry / rotation /
    block handling.

    `Client.get_text` cannot serve here twice over: it uses the requests session
    shiksha.com refuses, and it returns `resp.text`, which decodes a gzip FILE
    body as mojibake and then runs `is_block_page` over the garbage. Everything
    else — sticky session, wire-byte accounting, adaptive throttle, Retry-After,
    the shared `_on_failure` path — is reused rather than reworded.
    """
    sess = _session_for(client)
    last_err: Optional[Exception] = None
    for attempt in range(1, client.max_retries + 1):
        proxy = client.pm.get(client.session_id)
        try:
            resp = sess.get(url, proxies=_proxies(proxy),
                            timeout=client.timeout, allow_redirects=True)
            client.stats.add(requests=1, byts=_wire(resp))
            client._check_blocked(resp)
            resp.raise_for_status()
            raw = resp.content or b""
            # A challenge page arrives as HTTP 200 + HTML. Only worth testing
            # when the body is not a gzip container.
            if raw[:2] != b"\x1f\x8b" and is_block_page(
                    raw[:4096].decode("utf-8", "replace")):
                raise BlockedError("challenge/interstitial page (HTTP 200)")
            client.pm.report_success(proxy)
            if client.adaptive:
                client.adaptive.on_success()
            if client.verbose:
                client.log(f"   · GET …{url[-46:]} → {len(raw)//1024} KB")
            return raw
        # curl_cffi's RequestsError subclasses OSError, and requests'
        # RequestException subclasses IOError, so one clause covers both stacks.
        except (BlockedError, OSError, ValueError) as err:
            last_err = err
            client._on_failure(_transport_error(err), proxy, attempt, label)
    raise RuntimeError(f"{label} failed after {client.max_retries} attempts: {last_err}")


def exit_ip(client: Client, proxy) -> Optional[str]:
    """The public IP this request leaves from, or None if it can't be read."""
    # Deliberately OUTSIDE the try. A missing HTTP client is not "the echo
    # service is unreachable", and reporting it as one is worse than useless:
    # on 2026-09-25 a deploy that had not picked up curl_cffi printed
    # "⚠ could not confirm the proxy exit IP (echo unreachable) — continuing"
    # and only failed, with the real reason, several frames later. A broad
    # `except` that swallows a setup error turns a clear failure into a
    # misleading one.
    sess = _session_for(client)
    try:
        r = sess.get(IP_ECHO, proxies=_proxies(proxy),
                     timeout=20, allow_redirects=True)
        client.stats.add(requests=1, byts=_wire(r))
        if r.status_code != 200:
            return None
        return str((r.json() or {}).get("ip") or "") or None
    except Exception:  # noqa: BLE001
        return None


def assert_proxy_effective(client: Client, pm: ProxyManager, log) -> None:
    """Prove the proxy is actually carrying the traffic. Two requests.

    This is not belt-and-braces. curl_cffi is a DIFFERENT HTTP stack from the one
    every other vertical uses, and a proxy argument it does not honour would fail
    silently — the crawl would run happily from the Render host's own IP while
    the logs said 'gateway'. Given the owner's standing instruction ("use proxy
    always") and that Shiksha has never been crawled from here, a silent bypass
    is the worst available outcome: it burns the one IP we cannot rotate.

    So it is measured, not trusted. If the echo service cannot be reached the run
    continues with a warning — a third-party outage should not block a crawl —
    but a proxy that demonstrably is not changing the exit IP stops it.
    """
    if pm.mode == "none":
        return
    proxy = pm.get(client.session_id or "skcheck")
    if proxy is None:
        return
    via = exit_ip(client, proxy)
    own = exit_ip(client, None)
    if via and own and via == own:
        raise ProxyIneffective(
            f"the proxy is not carrying the traffic: requests through the "
            f"gateway and requests sent direct both leave from {via}. Refusing "
            f"to crawl Shiksha from this host's own IP.")
    if not via:
        log("  ⚠ could not confirm the proxy exit IP (echo unreachable) — "
            "continuing, but the proxy is unverified for this run")
    else:
        log(f"  proxy exit IP confirmed ({via}), distinct from this host's own")


# ---------------------------------------------------------------------------
# Config / client
# ---------------------------------------------------------------------------
def _proxy_cfg() -> Dict[str, Any]:
    """Use the SAME proxy settings the rest of the app is configured with."""
    try:
        g = _core.get_setting
        return {
            "proxy_mode": g("proxy_mode", "none"),
            "proxy_gateway": _core.proxy_gateway(),
            "proxy_list": [p.strip() for p in
                           (g("proxy_list_text", "") or "").splitlines() if p.strip()],
            "proxy_cooldown": g("proxy_cooldown", 120),
            "proxy_session_template": g("proxy_session_template", ""),
            "delay": float(g("delay", 1.0) or 1.0),
        }
    except Exception:  # noqa: BLE001
        return {"proxy_mode": "none", "proxy_gateway": "", "proxy_list": [],
                "proxy_cooldown": 120, "proxy_session_template": "", "delay": 1.0}


def _client(pm, cfg, log, stats, adaptive) -> Client:
    return Client(pm, log=log,
                  max_retries=int(cfg.get("max_retries", 5)),
                  backoff=float(cfg.get("backoff", 4)),
                  stats=stats, adaptive=adaptive)


class ProxyRequired(RuntimeError):
    """Raised instead of silently scraping Shiksha from the app's own IP."""


def require_proxy(pm: ProxyManager, cfg: Dict[str, Any], log) -> None:
    """*"also use proxy always"* — enforced, not documented.

    A config flag can override it, because the test harness drives these runners
    for real with the transport monkeypatched and no proxy configured. The
    override is loud: if a production run ever goes out direct, the job log says
    so in as many words."""
    if pm.mode != "none" and pm.healthy_count() > 0:
        return
    if cfg.get("allow_direct"):
        log("  ⚠ RUNNING WITHOUT A PROXY — allow_direct was set. Shiksha will see "
            "this host's own IP.")
        return
    raise ProxyRequired(
        "Shiksha is configured to run through a proxy only, and no working proxy "
        "is set (proxy_mode=" + str(pm.mode) + "). Configure the gateway in "
        "Settings, or set allow_direct in the job config to override.")


# ---------------------------------------------------------------------------
# Accumulators — a college's facts arrive spread across several sitemap files
# ---------------------------------------------------------------------------
class _Index:
    """Per-id union of everything discovery has seen, seeded from the db so a
    resumed run extends the stored value instead of shrinking it."""

    def __init__(self, seed: Optional[Dict[int, Dict[str, Any]]] = None) -> None:
        self.lock = threading.Lock()
        self.colleges: Dict[int, Dict[str, Any]] = {}
        self.dirty: set = set()
        for cid, v in (seed or {}).items():
            self.colleges[cid] = {"slug": v.get("slug") or "",
                                  "url": "", "tabs": set(v.get("tabs") or ()),
                                  "aliases": set(), "seeded_aliases":
                                      int(v.get("alias_count") or 0),
                                  "lastmod": ""}

    def _row(self, cid: int) -> Dict[str, Any]:
        r = self.colleges.get(cid)
        if r is None:
            r = {"slug": "", "url": "", "tabs": set(), "aliases": set(),
                 "seeded_aliases": 0, "lastmod": ""}
            self.colleges[cid] = r
        return r

    def see_college(self, cid: int, slug: str, url: str = "", tab: str = "",
                    lastmod: str = "", home: bool = False) -> None:
        with self.lock:
            r = self._row(cid)
            if slug:
                r["aliases"].add(slug)
                # Canonical = the shortest slug seen. A heuristic, and a stable
                # one: alias slugs on Shiksha are longer, city- or
                # course-qualified variants of the base slug. The detail phase
                # overwrites `name`/`url` from the page itself, so a wrong guess
                # here costs nothing but a cosmetic column.
                if home and (not r["slug"] or len(slug) < len(r["slug"])):
                    r["slug"], r["url"] = slug, url
            if tab:
                r["tabs"].add(tab)
            if lastmod and lastmod > (r["lastmod"] or ""):
                r["lastmod"] = lastmod
            self.dirty.add(cid)

    def drain(self, job_id: int) -> List[Dict[str, Any]]:
        """Rows for the ids touched since the last drain. `tabs` and
        `alias_count` are unions, so writing them is safe."""
        now = time.time()
        with self.lock:
            ids, self.dirty = self.dirty, set()
            out = []
            for cid in ids:
                r = self.colleges[cid]
                out.append({
                    "college_id": cid,
                    "slug": r["slug"],
                    "url": r["url"] or f"{SITE}/college/{r['slug']}-{cid}",
                    "tabs": ",".join(sorted(r["tabs"])),
                    "alias_count": max(len(r["aliases"]), r["seeded_aliases"]),
                    "lastmod": r["lastmod"],
                    "discovered_at": now,
                    "scraped_at": now,
                    "source_job_id": job_id,
                })
            return out


# ---------------------------------------------------------------------------
# Phase Ⓐ — sitemap discovery
# ---------------------------------------------------------------------------
def list_sitemaps(client: Client, groups: Iterable[str],
                  log) -> List[Tuple[str, str, str]]:
    """[(url, kind, lastmod)] for the sitemaps worth reading."""
    raw = fetch_bytes(client, SITEMAP_INDEX, "sitemap index")
    entries = parse_sitemap(decompress(raw))
    log(f"  sitemap index: {len(entries)} sitemaps advertised")
    picked: List[Tuple[str, str, str]] = []
    for loc, lastmod in entries:
        name = loc.rsplit("/", 1)[-1]
        for g in groups:
            if name.startswith(g):
                kind = ("college" if "college" in g else
                        "university" if "university" in g else "listing")
                picked.append((loc, kind, lastmod))
                break
    return picked


def run_discovery(job_id: int, cfg: Dict[str, Any],
                  log: Optional[Callable[[str], None]] = None) -> None:
    log = log or (lambda m: print(m, flush=True))
    merged = {**_proxy_cfg(), **cfg}
    pm = ProxyManager.from_config(merged)
    stats = Stats()
    adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                             enabled=bool(merged.get("adaptive", True)))
    concurrency = max(1, int(merged.get("concurrency", 4)))
    delay = float(merged.get("delay", 1.0))
    budget_requests = int(merged.get("budget_requests", 0))
    budget_bytes = int(float(merged.get("budget_mb", 0)) * 1024 * 1024)
    groups = merged.get("sitemap_groups") or SITEMAP_GROUPS
    if isinstance(groups, str):
        groups = [g.strip() for g in groups.split(",") if g.strip()]

    sk_db.update_job(job_id, status="running", message="reading sitemap index…")
    log(f"Shiksha · Phase Ⓐ discovery [BUILD {BUILD}] concurrency={concurrency}")
    require_proxy(pm, merged, log)

    boot = _client(pm, merged, log, stats, adaptive)
    boot.session_id = f"skboot{int(time.time())}"
    if not merged.get("skip_proxy_check"):
        assert_proxy_effective(boot, pm, log)
    sitemaps = list_sitemaps(boot, groups, log)
    log(f"  {len(sitemaps)} in scope: " + ", ".join(
        f"{k}×{sum(1 for _, kk, _ in sitemaps if kk == k)}"
        for k in ("college", "university", "listing")))

    if not merged.get("force_restart"):
        done = sk_db.done_sitemaps()
        skipped = len([s for s in sitemaps if s[0] in done])
        sitemaps = [s for s in sitemaps if s[0] not in done]
        if skipped:
            log(f"  resuming: {skipped} sitemaps already done")

    total = len(sitemaps)
    sk_db.update_job(job_id, total_units=total, message=f"{total} sitemaps to read")

    index = _Index(sk_db.load_college_index())
    log(f"  seeded from db: {len(index.colleges):,} colleges already known")

    q: "_queue.Queue" = _queue.Queue()
    for s in sitemaps:
        q.put(s)

    stop = threading.Event()
    lock = threading.Lock()
    state = {"done": 0, "rows": 0, "urls": 0, "offerings": 0, "univ": 0}
    halt = {"reason": None}

    def budget_hit() -> Optional[str]:
        reqs, byts, _ = stats.snapshot()
        if budget_requests and reqs >= budget_requests:
            return f"request budget reached ({reqs})"
        if budget_bytes and byts >= budget_bytes:
            return f"bandwidth budget reached ({byts/1048576:.1f} MB)"
        return None

    def push():
        reqs, byts, _ = stats.snapshot()
        with lock:
            d, rw = state["done"], state["rows"]
        sk_db.update_job(job_id, done_units=d, items_written=rw, req_count=reqs,
                         bytes_count=byts,
                         message=f"{d}/{total} sitemaps · {rw:,} rows · "
                                 f"{byts/1048576:.1f} MB")

    def flush_colleges() -> int:
        rows = index.drain(job_id)
        for i in range(0, len(rows), FLUSH_EVERY):
            sk_db.upsert_colleges(rows[i:i + FLUSH_EVERY])
        return len(rows)

    def worker(idx: int):
        client = _client(pm, merged, log, stats, adaptive)
        while not stop.is_set():
            try:
                url, kind, lastmod = q.get_nowait()
            except _queue.Empty:
                return
            if sk_db.stop_requested(job_id):
                halt["reason"] = "stopped by user"
                stop.set()
                return
            bh = budget_hit()
            if bh:
                log(f"  ⏸ {bh}")
                halt["reason"] = bh
                stop.set()
                return
            # One sticky exit IP per sitemap — the same rule every other phase
            # follows. Without a session id Client._rotate() is a no-op and a
            # single refused IP takes the whole worker down with it (the Phase 3
            # failure of 2026-09-21).
            client.session_id = f"sk{idx}_{abs(hash(url)) % 10**8}"
            name = url.rsplit("/", 1)[-1]
            try:
                raw = fetch_bytes(client, url, f"sitemap {name}")
                locs = parse_sitemap(decompress(raw))
                aliases: Dict[str, Dict[str, Any]] = {}
                universities: Dict[int, Dict[str, Any]] = {}
                courses: Dict[int, Dict[str, Any]] = {}
                offerings: Dict[Tuple[int, int], Dict[str, Any]] = {}
                n_col = n_univ = n_off = 0
                now = time.time()

                for loc, lm in locs:
                    c = classify(loc)
                    k = c["kind"]
                    if k in ("college_home", "college_tab", "offering"):
                        cid, slug = c.get("college_id"), c.get("slug") or ""
                        if cid is None:
                            continue
                        index.see_college(cid, slug, url=loc,
                                          tab=c.get("tab") or "",
                                          lastmod=lm, home=(k == "college_home"))
                        if k == "college_home":
                            n_col += 1
                            aliases.setdefault(slug, {
                                "slug": slug, "college_id": cid, "url": loc,
                                "first_seen_at": now, "source_job_id": job_id})
                        if k == "offering":
                            crs, cslug = c.get("course_id"), c.get("course_slug") or ""
                            if crs is None:
                                continue
                            n_off += 1
                            offerings[(cid, crs)] = {
                                "college_id": cid, "course_id": crs, "url": loc,
                                "course_slug": cslug, "lastmod": lm,
                                "discovered_at": now, "scraped_at": now,
                                "source_job_id": job_id}
                            courses.setdefault(crs, {
                                "course_id": crs, "slug": cslug,
                                "name": cslug.replace("-", " ").title(),
                                "discovered_at": now, "scraped_at": now,
                                "source_job_id": job_id})
                    elif k in ("university_home", "university_sub"):
                        uid = c.get("university_id")
                        if uid is None:
                            continue
                        if k == "university_home":
                            n_univ += 1
                            universities[uid] = {
                                "university_id": uid, "slug": c.get("slug") or "",
                                "url": loc, "lastmod": lm, "discovered_at": now,
                                "scraped_at": now, "source_job_id": job_id}

                written = flush_colleges()
                if aliases:
                    sk_db.upsert_aliases(list(aliases.values()))
                if universities:
                    sk_db.upsert_universities(list(universities.values()))
                if courses:
                    sk_db.upsert_courses(list(courses.values()))
                if offerings:
                    vals = list(offerings.values())
                    for i in range(0, len(vals), FLUSH_EVERY):
                        sk_db.upsert_offerings(vals[i:i + FLUSH_EVERY])

                sk_db.set_sitemap(url, kind, "done", urls=len(locs),
                                  colleges=n_col, universities=n_univ,
                                  offerings=n_off, bytes_=len(raw), lastmod=lastmod)
                with lock:
                    state["rows"] += written + len(offerings) + len(universities)
                    state["urls"] += len(locs)
                    state["offerings"] += len(offerings)
                    state["univ"] += len(universities)
                log(f"  ✓ {name}: {len(locs):,} URLs → {n_col:,} college homes, "
                    f"{n_off:,} offerings, {n_univ:,} universities")
            except Exception as err:  # noqa: BLE001
                log(f"  ! sitemap {name} failed: {err}")
                sk_db.set_sitemap(url, kind, "error", message=str(err)[:400])
            with lock:
                state["done"] += 1
            push()
            if delay:
                time.sleep(adaptive.value() if adaptive else delay)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    flush_colleges()
    sk_db.recount_course_colleges()
    push()

    c = sk_db.counts()
    fc = sk_db.detail_forecast()
    msg = (f"{halt['reason'] + ' — ' if halt['reason'] else ''}"
           f"discovery: {c['colleges']:,} colleges ({c['aliases']:,} alias slugs), "
           f"{c['universities']:,} universities, {c['courses']:,} courses, "
           f"{c['offerings']:,} offerings · "
           f"phase B forecast: {fc['colleges_left']:,} colleges ≈ "
           f"{fc['est_gb_left']} GB at {fc['kb_per_college']:.0f} KB each")
    sk_db.update_job(job_id, status="stopped" if halt["reason"] else "completed",
                     message=msg, finished_at=time.time())
    log(msg)
