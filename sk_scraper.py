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
import queue as _queue
import re
import threading
import time
import zlib
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import db as _core
import sk_db
from scraper import (AdaptiveDelay, BlockedError, Client, ProxyManager, Stats,
                     base_headers, is_block_page, redact_proxy, wire_bytes)

SITE = "https://www.shiksha.com"
SITEMAP_INDEX = f"{SITE}/sitemap_index.xml"

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


def fetch_bytes(client: Client, url: str, label: str) -> bytes:
    """GET raw bytes through the proxy with the client's own retry / rotation /
    block handling.

    `Client.get_text` cannot serve here: it returns `resp.text`, which decodes a
    gzip FILE body as mojibake, and it runs `is_block_page` over that garbage.
    Everything else — sticky session, wire-byte accounting, adaptive throttle,
    Retry-After, the shared `_on_failure` path — is reused rather than reworded,
    so this cannot drift away from the four fetchers in `scraper`.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, client.max_retries + 1):
        proxy = client.pm.get(client.session_id)
        try:
            resp = client.session.get(
                url, headers=base_headers(),
                proxies=proxy.as_dict() if proxy else None,
                timeout=client.timeout, stream=True)
            client.stats.add(requests=1, byts=wire_bytes(resp))
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
        except Exception as err:  # noqa: BLE001
            if not isinstance(err, (BlockedError, IOError, OSError, ValueError)) \
                    and type(err).__module__.split(".")[0] != "requests":
                raise
            last_err = err
            client._on_failure(err, proxy, attempt, label)
    raise RuntimeError(f"{label} failed after {client.max_retries} attempts: {last_err}")


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
