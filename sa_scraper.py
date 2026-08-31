"""
Study Abroad — scraping engine (parsers + phase runners).

REUSES generic infrastructure from the existing `scraper` module WITHOUT modifying
it: the HTTP `Client` (proxy, retry, backoff, IP rotation, block detection),
`ProxyManager`, `encode_payload`, `abs_url`, numeric helpers, and the
`_nextdata_pageprops` SSR extractor. All Study-Abroad-specific logic (endpoint,
payload keys, parsing, partitioning, phases) lives here and writes only to the
isolated SA database via `sa_db`.

Confirmed by live reverse-engineering:
  - Listing API: GET web-api/listing-cf-sa?data=base64({"page":N, <filter>:<id>, ...})
  - 20 results/page, hasNext flag, count; per-query result cap ~10,000 (~page 500)
  - Program row shape (see sa_parse_program) with native + INR fees.
"""
from __future__ import annotations

BUILD = "2026-07-23a"

import json
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

import sa_db
import db as _core  # read SHARED proxy/rate settings (used as-is by SA)
# Generic infra reused from the domestic engine (imported, never modified):
from scraper import (Client, ProxyManager, encode_payload, abs_url,
                     _to_int, _nextdata_pageprops, BudgetExceeded,
                     Stats, AdaptiveDelay, send_notification)

SA_LISTING_API = "https://collegedunia.com/web-api/listing-cf-sa"
SITE = "https://collegedunia.com"
COURSE_FINDER_URL = f"{SITE}/study-abroad/course-finder"
IMG_BASE = "https://image-static.collegedunia.com/public/college_data/images/studyabroad/logos/"
PAGE_SIZE = 20
RESULT_CAP = 10000                    # observed per-query cap (~page 500)
MAX_PAGES = RESULT_CAP // PAGE_SIZE    # 500 — hard guard
# Dimensions used to recursively split a query until each slice is under the cap.
SPLIT_ORDER = ["country", "course_type", "stream", "head_short_form"]


SCHOLARSHIP_URL = f"{SITE}/scholarship"
SCHOLARSHIP_PAGE_SIZE = 21          # observed rows per listing page
MAX_SCHOLARSHIP_PAGES = 200         # safety cap (29 pages observed)


class StopRequested(Exception):
    pass


# ---------------------------------------------------------------------------
# Scholarships (collegedunia.com/scholarship)
# ---------------------------------------------------------------------------
# Both the listing row's `content` and the detail page's `highlights` are
# [{label, value}] blocks. One label map serves both.
_SCHOL_LABELS = {
    "amount": "amount_text",
    "type": "scholarship_type",
    "scholarship type": "scholarship_type",
    "level of study": "level_of_study",
    "offered by": "offered_by",
    "organization": "organization",
    "application deadline": "deadline",
    "deadline": "deadline",
    "number of scholarships": "num_scholarships",
    "renewability": "renewability",
    "international student eligible": "international_eligible",
    "scholarship website link": "website_link",
}
_INR_RE = re.compile(r"₹\s*([\d,]+)")
_PAREN_RE = re.compile(r"\(([^)]+)\)")


def parse_scholarship_amount(text: Optional[str]):
    """'₹1,436,100 ($15,000)' -> (1436100, 15000, 'USD')."""
    if not text:
        return None, None, None
    inr = None
    m = _INR_RE.search(text)
    if m:
        inr = parse_amount(m.group(1))
    native_amt = native_cur = None
    p = _PAREN_RE.search(text)
    if p:
        native_cur, native_amt = parse_native_fee(p.group(1))
    return inr, native_amt, native_cur


def _label_block(items) -> Dict[str, Any]:
    """Flatten a [{label, value, url?}] block onto our column names."""
    out: Dict[str, Any] = {}
    for it in (items or []):
        if not isinstance(it, dict):
            continue
        col = _SCHOL_LABELS.get(str(it.get("label") or "").strip().lower())
        if not col:
            continue
        val = it.get("url") or it.get("value")
        if val in (None, "", "N/A"):
            continue
        out[col] = str(val).strip()
    if "num_scholarships" in out:
        out["num_scholarships"] = _to_int(out["num_scholarships"])
    if out.get("amount_text"):
        inr, nat, cur = parse_scholarship_amount(out["amount_text"])
        out["amount_inr"], out["amount_native"], out["amount_currency"] = inr, nat, cur
    return out


def sa_parse_scholarship(s: Dict[str, Any], job_id: Optional[int] = None) -> Dict[str, Any]:
    """One listing row: {id, title, url, content:[{label,value}]}."""
    row = {
        "scholarship_id": _to_int(s.get("id")),
        "title": (s.get("title") or "").strip(),
        "url": abs_url(s.get("url")),
        "raw_json": json.dumps(s, ensure_ascii=False),
        "scraped_at": time.time(),
        "source_job_id": job_id,
    }
    row.update(_label_block(s.get("content")))
    return row


def sa_parse_scholarship_detail(pageprops: Dict[str, Any]) -> Dict[str, Any]:
    """Detail page -> highlights + the HTML eligibility/application/selection
    blocks + the article description."""
    r = (pageprops or {}).get("response") or {}
    out = _label_block(r.get("highlights"))
    art = r.get("article") or {}
    if isinstance(art, dict) and art.get("description"):
        out["description"] = str(art["description"])[:200000]
    for key in ("eligibility", "application", "selection"):
        v = r.get(key)
        if isinstance(v, str) and v.strip():
            out[key] = v[:200000]
        elif isinstance(v, (list, dict)) and v:
            out[key] = json.dumps(v, ensure_ascii=False)[:200000]
    countries = r.get("by_countries")
    if isinstance(countries, list) and countries:
        out["countries"] = ", ".join(
            str(c.get("name") or "").replace("Scholarships in ", "").strip()
            for c in countries if isinstance(c, dict) and c.get("name"))
    return out


def fetch_scholarship_page(client: "Client", page: int) -> Dict[str, Any]:
    """One listing page. Data is server-rendered into __NEXT_DATA__ (there is no
    JSON endpoint for this section), so this reads pageProps.response."""
    url = SCHOLARSHIP_URL if page <= 1 else f"{SCHOLARSHIP_URL}?page={int(page)}"
    pp = _nextdata_pageprops(client.get_text(url))
    return (pp or {}).get("response") or {}


# ---------------------------------------------------------------------------
# Parsing helpers (currency-aware)
# ---------------------------------------------------------------------------
_CUR_SYMBOL = {"$": "USD", "£": "GBP", "€": "EUR", "₹": "INR", "¥": "JPY", "₩": "KRW"}
_UNIT_MULT = {"cr": 1e7, "crore": 1e7, "lakh": 1e5, "lac": 1e5, "l": 1e5,
              "k": 1e3, "m": 1e6, "mn": 1e6, "b": 1e9}


def parse_amount(s: Optional[str]) -> Optional[int]:
    """'88,800' -> 88800 ; '1.2 Cr' -> 12000000 ; '50 K' -> 50000."""
    if not s:
        return None
    t = s.replace(",", "")
    m = re.search(r"([\d.]+)\s*(cr|crore|lakh|lac|l|k|mn|m|b)?", t, re.I)
    if not m:
        return None
    try:
        n = float(m.group(1))
    except ValueError:
        return None
    mult = _UNIT_MULT.get((m.group(2) or "").lower(), 1)
    return int(round(n * mult))


def parse_native_fee(s: Optional[str]):
    """'GBP 88,800' -> ('GBP', 88800) ; '$50,000' -> ('USD', 50000)."""
    if not s:
        return None, None
    cur = None
    m = re.match(r"\s*([A-Z]{3})\b", s)
    if m:
        cur = m.group(1)
    else:
        for sym, code in _CUR_SYMBOL.items():
            if sym in s:
                cur = code
                break
    return cur, parse_amount(s)


def parse_duration_months(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"([\d.]+)\s*(years?|yrs?|months?|mos?|weeks?)", s, re.I)
    if not m:
        return None
    n = float(m.group(1)); u = m.group(2).lower()
    if u.startswith("y"):
        return int(round(n * 12))
    if u.startswith("mo") or u.startswith("month"):
        return int(round(n))
    if u.startswith("w"):
        return int(round(n / 4.345))
    return None


def _country_from_url(url: str) -> str:
    if not url:
        return ""
    u = url.replace("https://collegedunia.com/", "").lstrip("/")
    seg = u.split("/")[0].strip().lower()
    return seg if seg and seg != "university" else ""


def _logo_url(logo: Optional[str]) -> str:
    if not logo:
        return ""
    return logo if logo.startswith("http") else IMG_BASE + logo


# ---------------------------------------------------------------------------
# Program parser — captures MAXIMUM fields
# ---------------------------------------------------------------------------
def sa_parse_program(c: Dict[str, Any], job_id: Optional[int] = None) -> Dict[str, Any]:
    lp = c.get("lead_params") or {}
    rank = c.get("ranking") or {}
    if isinstance(rank, list):
        rank = {}
    cu = c.get("college_url") or ""
    cur, native_amt = parse_native_fee(c.get("default_fee_per_year"))
    return {
        "program_id": _to_int(c.get("course_id")),
        "name": c.get("head_one") or "",
        "name_secondary": c.get("head_two") or "",
        "course_tags": c.get("course_tags") or "",
        "program_type": c.get("program_type") or "",
        "duration_text": c.get("course_duration") or "",
        "duration_months": parse_duration_months(c.get("course_duration")),
        "languages": c.get("course_languages") or "",
        "is_stem": 1 if c.get("is_stem") else 0,
        "is_partner": 1 if c.get("is_partner") else 0,
        "fee_native_raw": c.get("default_fee_per_year") or "",
        "fee_currency": cur,
        "fee_native_amount": native_amt,
        "fee_inr_raw": c.get("total_fee_per_year") or "",
        "fee_inr_amount": parse_amount(c.get("total_fee_per_year")),
        "fee_period": "per_year",
        "application_end_date": c.get("application_end_date") or "",
        "university_id": _to_int(lp.get("college_id")),
        "university_name": c.get("college_name") or "",
        "university_url": abs_url(cu),
        "country_code": _country_from_url(cu),
        "program_url": abs_url(c.get("course_link")),   # <-- enrichment target
        "logo_url": _logo_url(c.get("college_logo")),
        "ranking_rank": _to_int(rank.get("rank")),
        "ranking_out_of": _to_int(rank.get("rank_out_of")),
        "ranking_scope": rank.get("scope") or "",
        "ranking_agency": rank.get("agencyName") or rank.get("agencyShortForm") or "",
        "ranking_year": _to_int(rank.get("year")),
        "description": (c.get("course_description") or "")[:4000],
        "raw_json": json.dumps(c, ensure_ascii=False),
        "scraped_at": time.time(),
        "source_job_id": job_id,
    }


def sa_derive_university(c: Dict[str, Any], job_id: Optional[int] = None) -> Dict[str, Any]:
    lp = c.get("lead_params") or {}
    cu = c.get("college_url") or ""
    return {
        "university_id": _to_int(lp.get("college_id")),
        "name": c.get("college_name") or "",
        "country_code": _country_from_url(cu),
        "city": "",
        "university_url": abs_url(cu),
        "logo_url": _logo_url(c.get("college_logo")),
        "raw_json": "", "scraped_at": time.time(), "source_job_id": job_id,
    }


def sa_derive_exams(c: Dict[str, Any]) -> List[Dict[str, Any]]:
    pid = _to_int(c.get("course_id"))
    out = []
    for e in (c.get("exams_data") or []):
        if not isinstance(e, dict):
            continue
        out.append({
            "program_id": pid,
            "exam_name": e.get("entrance_name") or "",
            "short_form": e.get("short_form") or "",
            "exam_score": str(e.get("exam_score") or ""),
            "out_of": str(e.get("exam_out_of_score") or ""),
            "median": str(e.get("median_score") or ""),
            "url": abs_url(e.get("url")),
        })
    return out


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def fetch_listing(client: "Client", filters: Dict[str, Any], page: int) -> Dict[str, Any]:
    payload = {"page": int(page), **filters}
    url = f"{SA_LISTING_API}?data={encode_payload(payload)}"
    txt = client.get_text(url)
    try:
        return json.loads(txt)
    except (json.JSONDecodeError, ValueError) as err:
        raise RuntimeError(f"non-JSON listing-cf-sa response: {str(err)[:80]}")


def shared_proxy_cfg() -> Dict[str, Any]:
    """Read the SAME proxy / rate-limit settings the domestic vertical uses (from
    the shared `settings` table) so Study Abroad uses the configured gateway AS-IS.
    No separate proxy config for SA."""
    try:
        g = _core.get_setting
        return {
            "proxy_mode": g("proxy_mode", "none"),
            "proxy_gateway": _core.proxy_gateway(),
            "proxy_list": [p.strip() for p in (g("proxy_list_text", "") or "").splitlines() if p.strip()],
            "proxy_cooldown": g("proxy_cooldown", 120),
            "delay": float(g("delay", 1.0) or 1.0),
        }
    except Exception:  # noqa: BLE001  (settings table not present yet -> direct)
        return {"proxy_mode": "none", "proxy_gateway": "", "proxy_list": [],
                "proxy_cooldown": 120, "delay": 1.0}


def _build_client(cfg: Dict[str, Any], log) -> "Client":
    # Shared proxy/rate settings first; the job cfg may override (e.g. budget).
    merged = {**shared_proxy_cfg(), **cfg}
    pm = ProxyManager.from_config(merged)
    stats = Stats()
    adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                             enabled=bool(merged.get("adaptive", True)))
    return Client(pm, log=log, max_retries=int(merged.get("max_retries", 5)),
                  backoff=float(merged.get("backoff", 4)), stats=stats, adaptive=adaptive)


# ---------------------------------------------------------------------------
# Facets (partition dimensions) — from the SSR __NEXT_DATA__
# ---------------------------------------------------------------------------
def run_facets(job_id: int, cfg: Dict[str, Any], log: Callable[[str], None]) -> None:
    sa_db.update_job(job_id, status="running", message="fetching facets")
    client = _build_client(cfg, log)
    _ensure_facets(client, job_id, log, force=True)
    sa_db.update_job(job_id, status="completed", finished_at=time.time(),
                     message="facets captured")


def _ensure_facets(client, job_id, log, force=False):
    have = sa_db.get_setting("facets_captured", False)
    if have and not force:
        return
    log("Fetching course-finder facets (SSR __NEXT_DATA__)…")
    html = client.get_text(COURSE_FINDER_URL)
    pp = _nextdata_pageprops(html)
    filters = ((pp.get("filterResponse") or {}).get("filters")) or {}
    total = (pp.get("listingResponse") or {}).get("count")
    rows = []
    for fname, fobj in filters.items():
        for v in (fobj.get("values") or []):
            rows.append({"filter_name": fname, "value_id": str(v.get("value")),
                         "label": v.get("text") or "", "count": _to_int(v.get("count")) or 0,
                         "updated_at": time.time()})
    sa_db.save_facets(rows)
    sa_db.set_setting("facets_captured", True)
    sa_db.set_setting("total_programs", total)
    log(f"  captured {len(rows)} facet values across {len(filters)} filters "
        f"(total programs ~{total}).")


def _split_values(dim: str) -> List[str]:
    with sa_db.connect() as conn:
        return [r[0] for r in conn.execute(
            "SELECT value_id FROM sa_facets WHERE filter_name=? ORDER BY count DESC",
            (dim,)).fetchall()]


def _pkey(filters: Dict[str, Any]) -> str:
    return "|".join(f"{k}={filters[k]}" for k in sorted(filters)) or "ALL"


# ---------------------------------------------------------------------------
# Phase: Programs — adaptive partitioned crawl with resume
# ---------------------------------------------------------------------------
def run_programs(job_id: int, cfg: Dict[str, Any], log: Callable[[str], None]) -> None:
    merged = {**shared_proxy_cfg(), **cfg}
    shared_stats = Stats()
    shared_adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                                    enabled=bool(merged.get("adaptive", True)))

    def make_client() -> "Client":
        pm = ProxyManager.from_config(merged)
        return Client(pm, log=log, max_retries=int(merged.get("max_retries", 5)),
                      backoff=float(merged.get("backoff", 4)),
                      stats=shared_stats, adaptive=shared_adaptive)

    disc_client = make_client()
    _ensure_facets(disc_client, job_id, log)
    concurrency = max(1, int(cfg.get("concurrency", 1)))
    budget_bytes = int(float(cfg.get("budget_mb", 0)) * 1024 * 1024)
    budget_reqs = int(cfg.get("budget_requests", 0) or 0)
    incremental = bool(cfg.get("incremental_promote", True)) and bool(cfg.get("staging", True))
    done = sa_db.done_partitions()
    state = {"written": 0, "partitions": 0}
    lock = threading.Lock()
    stop_flag = {"stop": False}

    sa_db.update_job(job_id, status="running", message="discovering partitions")

    def budget_hit() -> bool:
        reqs, byts, _ = shared_stats.snapshot()
        if budget_bytes and byts >= budget_bytes:
            return True
        if budget_reqs and reqs >= budget_reqs:
            return True
        return False

    def write_page(data):
        courses = data.get("courses") or []
        if not courses:
            return 0
        progs = [sa_parse_program(c, job_id) for c in courses]
        univs = [sa_derive_university(c, job_id) for c in courses]
        exams = []
        for c in courses:
            exams += sa_derive_exams(c)
        seen = {}
        for p in progs:
            cc = p.get("country_code")
            if cc and cc not in seen:
                seen[cc] = {"country_code": cc, "country_id": None, "name": cc.upper(),
                            "raw_json": "", "scraped_at": time.time(), "source_job_id": job_id}
        # governance: route to staging (default) or straight to master
        sa_db.write_rows(job_id, cfg, "sa_programs", progs)
        sa_db.write_rows(job_id, cfg, "sa_universities", univs)
        sa_db.write_rows(job_id, cfg, "sa_program_exams", exams)
        sa_db.write_rows(job_id, cfg, "sa_countries", list(seen.values()))
        return len(progs)

    def maybe_flush():
        if incremental:
            try:
                sa_db.flush_job_staging(job_id)
            except Exception:  # noqa: BLE001
                pass

    def page_partition(cl, filters, key):
        """Page one leaf partition to hasNext=false (or the ~10k cap). Thread-safe:
        `found` is local; only shared counters/flush are guarded by `lock`."""
        prog = sa_db.get_progress(key) or {}
        start = 1
        if prog.get("status") == "partial" and prog.get("last_page"):
            start = int(prog["last_page"]) + 1
        found = int(prog.get("found") or 0)
        page = start
        while page <= MAX_PAGES:
            if stop_flag["stop"] or sa_db.stop_requested(job_id):
                stop_flag["stop"] = True
                raise StopRequested()
            if budget_hit():
                stop_flag["stop"] = True
                raise BudgetExceeded("budget reached")
            data = fetch_listing(cl, filters, page)
            n = write_page(data)
            found += n
            sa_db.set_progress(key, "partial", page, found)
            with lock:
                state["written"] += n
                maybe_flush()
                reqs, byts, _ = shared_stats.snapshot()
            sa_db.update_job(job_id, done_units=state["written"], items_written=state["written"],
                             req_count=reqs, bytes_count=byts,
                             message=f"{key} p{page}: +{n} (total {state['written']})")
            log(f"  {key} p{page}: +{n} (partition total {found})")
            if not data.get("hasNext", False):
                break
            page += 1
            time.sleep(cl.adaptive.value() if cl.adaptive else 1.0)  # adaptive throttle
        sa_db.set_progress(key, "done", page, found)
        with lock:
            state["partitions"] += 1

    # ---- Discovery: recursively split until each leaf slice is under the cap ----
    leaves: List = []

    def discover(filters, di):
        if stop_flag["stop"] or sa_db.stop_requested(job_id):
            stop_flag["stop"] = True
            return
        key = _pkey(filters)
        if key in done:
            return
        cnt = int(fetch_listing(disc_client, filters, 1).get("count") or 0)
        if cnt == 0:
            sa_db.set_progress(key, "done", 1, 0)
            return
        if cnt <= RESULT_CAP or di >= len(SPLIT_ORDER):
            if cnt > RESULT_CAP:
                log(f"  ! {key} {cnt:,} > cap {RESULT_CAP:,}, no more split dims — first ~{RESULT_CAP:,} only")
            leaves.append((filters, key))
        else:
            vals = _split_values(SPLIT_ORDER[di])
            if not vals:
                leaves.append((filters, key))
                return
            for v in vals:
                discover({**filters, SPLIT_ORDER[di]: v}, di + 1)

    def do_leaf(item):
        if stop_flag["stop"]:
            return
        filters, key = item
        cl = make_client() if concurrency > 1 else disc_client
        page_partition(cl, filters, key)

    def crawl_all():
        discover({}, 0)
        log(f"{len(leaves)} leaf partitions to crawl · concurrency={concurrency}")
        sa_db.update_job(job_id, total_units=len(leaves), message=f"{len(leaves)} partitions")
        if concurrency > 1 and len(leaves) > 1:
            with ThreadPoolExecutor(max_workers=concurrency) as ex:
                for fut in [ex.submit(do_leaf, it) for it in leaves]:
                    fut.result()  # propagate the first exception (Stop/Budget/error)
        else:
            for it in leaves:
                do_leaf(it)

    def finalize(base_msg: str):
        """QC the staged data and promote (mirrors the domestic _finalize_job)."""
        if not cfg.get("staging", True):
            sa_db.update_job(job_id, status="completed", finished_at=time.time(), message=base_msg)
            return
        v = sa_db.validate_job(job_id, cfg.get("validation_rules") or {})
        auto = cfg.get("auto_promote", True)
        if incremental or (v["passed"] and auto):
            summ = sa_db.flush_job_staging(job_id)
            tag = ("promoted (incremental)" if incremental
                   else f"QC {v['score']:.0f}/100 ✓ promoted ({sum(summ.values())} rows)")
            msg = f"{base_msg} · {tag}"
            sa_db.update_job(job_id, status="completed", finished_at=time.time(),
                             quality_score=v["score"], message=msg)
            log(msg)
            send_notification(cfg, "Study Abroad job promoted", msg, log)
        else:
            why = "failed QC" if not v["passed"] else "auto-promote off"
            msg = f"{base_msg} · QC {v['score']:.0f}/100 — staged, awaiting approval ({why})"
            sa_db.update_job(job_id, status="completed", finished_at=time.time(),
                             quality_score=v["score"], message=msg)
            log(msg)
            send_notification(cfg, "Study Abroad job needs review", msg, log)

    try:
        crawl_all()
        finalize(f"done: {state['written']} programs, {state['partitions']} partitions")
        log(f"Done. {state['written']} programs across {state['partitions']} partitions.")
    except StopRequested:
        sa_db.update_job(job_id, status="stopped", finished_at=time.time(),
                         message=f"stopped by user ({state['written']} programs so far)")
        log("Stopped by user.")
    except BudgetExceeded as e:
        sa_db.update_job(job_id, status="stopped", finished_at=time.time(),
                         message=f"budget reached ({state['written']} programs); resume to continue")
        log(f"Budget stop: {e}")
    except Exception as e:  # noqa: BLE001
        sa_db.update_job(job_id, status="error", finished_at=time.time(),
                         message=f"{str(e)[:200]}")
        log(f"ERROR: {e}")
        raise


# ---------------------------------------------------------------------------
# Phase: Scholarships — listing sweep, then per-scholarship detail enrichment
# ---------------------------------------------------------------------------
def run_scholarships(job_id: int, cfg: Dict[str, Any], log: Callable[[str], None]) -> None:
    """Two passes on one job:

      1. LISTING  — walk /scholarship?page=N to the last page, staging one row
                    per scholarship (id, title, url + the content label block).
      2. DETAIL   — for every scholarship whose detail page has not been fetched,
                    read /scholarship/<id>-<slug> for highlights, eligibility,
                    application, selection and the description.

    The detail pass is driven by a self-draining query (detail_scraped_at IS
    NULL), so an interrupted run resumes with no separate progress table. Detail
    writes never blank a value the listing already provided.
    """
    merged = {**shared_proxy_cfg(), **cfg}
    stats = Stats()
    adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                             enabled=bool(merged.get("adaptive", True)))
    client = Client(ProxyManager.from_config(merged), log=log,
                    max_retries=int(merged.get("max_retries", 5)),
                    backoff=float(merged.get("backoff", 4)),
                    stats=stats, adaptive=adaptive)
    budget_bytes = int(float(cfg.get("budget_mb", 0)) * 1024 * 1024)
    budget_reqs = int(cfg.get("budget_requests", 0) or 0)
    incremental = bool(cfg.get("incremental_promote", True)) and bool(cfg.get("staging", True))
    want_details = bool(cfg.get("fetch_details", True))
    max_pages = int(cfg.get("max_pages", MAX_SCHOLARSHIP_PAGES) or MAX_SCHOLARSHIP_PAGES)

    def budget_hit() -> bool:
        reqs, byts, _ = stats.snapshot()
        return bool((budget_bytes and byts >= budget_bytes)
                    or (budget_reqs and reqs >= budget_reqs))

    def maybe_flush():
        if incremental:
            try:
                sa_db.flush_job_staging(job_id)
            except Exception:  # noqa: BLE001
                pass

    listed = detailed = 0
    try:
        # ---------------- pass 1: listing ----------------
        sa_db.update_job(job_id, status="running", message="scholarships: listing…")
        first = fetch_scholarship_page(client, 1)
        total = _to_int(first.get("total_count")) or 0
        last = _to_int((first.get("paginate") or {}).get("last")) or 1
        last = min(last, max_pages)
        sa_db.update_job(job_id, total_units=total,
                         message=f"{total} scholarships across {last} pages")
        log(f"Scholarships: {total} total, {last} listing pages.")

        page, data = 1, first
        while page <= last:
            if sa_db.stop_requested(job_id):
                raise StopRequested()
            if budget_hit():
                raise BudgetExceeded("budget reached")
            if data is None:
                data = fetch_scholarship_page(client, page)
            rows = [sa_parse_scholarship(s, job_id)
                    for s in (data.get("scholarships") or [])
                    if isinstance(s, dict) and s.get("id")]
            if rows:
                sa_db.write_rows(job_id, cfg, "sa_scholarships", rows)
                listed += len(rows)
            maybe_flush()
            reqs, byts, _ = stats.snapshot()
            sa_db.update_job(job_id, done_units=listed, items_written=listed,
                             req_count=reqs, bytes_count=byts,
                             message=f"listing p{page}/{last} · {listed} scholarships")
            log(f"  listing p{page}/{last}: +{len(rows)} (total {listed})")
            if not rows:
                break
            page += 1
            data = None
            time.sleep(adaptive.value())

        maybe_flush()   # details read from master, so promote the listing first

        # ---------------- pass 2: detail ----------------
        if want_details:
            pending = sa_db.scholarships_pending_detail(limit=cfg.get("limit"))
            log(f"Scholarship details: {len(pending)} pending.")
            sa_db.update_job(job_id, total_units=len(pending) or listed,
                             message=f"details: 0/{len(pending)}")
            for i, s in enumerate(pending, start=1):
                if sa_db.stop_requested(job_id):
                    raise StopRequested()
                if budget_hit():
                    raise BudgetExceeded("budget reached")
                try:
                    pp = _nextdata_pageprops(client.get_text(s["url"]))
                    fields = sa_parse_scholarship_detail(pp)
                    sa_db.update_scholarship_details(s["scholarship_id"], fields)
                    if fields:
                        detailed += 1
                except Exception as err:  # noqa: BLE001
                    # Blocked/failed pages keep detail_scraped_at NULL, so they
                    # stay in the queue for the next run rather than being lost.
                    log(f"  scholarship {s['scholarship_id']} err: {str(err)[:70]}")
                if i % 10 == 0:
                    reqs, byts, _ = stats.snapshot()
                    sa_db.update_job(job_id, done_units=i, items_written=detailed,
                                     req_count=reqs, bytes_count=byts,
                                     message=f"details {i}/{len(pending)} · {detailed} enriched")
                time.sleep(adaptive.value())

        reqs, byts, _ = stats.snapshot()
        msg = (f"done: {listed} scholarships listed"
               + (f", {detailed} detailed" if want_details else "")
               + f", {byts/1048576:.1f} MB")
        if cfg.get("staging", True):
            try:
                sa_db.flush_job_staging(job_id)
            except Exception:  # noqa: BLE001
                pass
        sa_db.update_job(job_id, status="completed", finished_at=time.time(),
                         req_count=reqs, bytes_count=byts, message=msg)
        log(msg)
        send_notification(cfg, "Study Abroad scholarships complete", msg, log)
    except StopRequested:
        maybe_flush()
        sa_db.update_job(job_id, status="stopped", finished_at=time.time(),
                         message=f"stopped by user ({listed} listed, {detailed} detailed)")
        log("Stopped by user.")
    except BudgetExceeded as e:
        maybe_flush()
        sa_db.update_job(job_id, status="stopped", finished_at=time.time(),
                         message=f"budget reached ({listed} listed, {detailed} detailed); resume to continue")
        log(f"Budget stop: {e}")
    except Exception as e:  # noqa: BLE001
        maybe_flush()
        sa_db.update_job(job_id, status="error", finished_at=time.time(),
                         message=str(e)[:200])
        log(f"ERROR: {e}")
        raise


# ===========================================================================
# Phase ④ — UNIVERSITY DETAIL
#
# `sa_universities` was only ever derived from programme rows: name, country,
# URL, logo — with `city` 0% filled. Its own page carries ~40 more fields plus a
# full multi-agency ranking history, a cost-of-living breakdown, nearby places,
# and a per-course block with SEVEN YEARS of fee history and multi-stage
# application deadlines.
#
# It also carries `exam_out_of_score`, which the programme listing sends blank —
# the reason sa_program_exams.out_of is 0% filled across 314k rows.
#
# 1,722 universities, one request each.
# ===========================================================================
UNIV_RANK_STREAM_KEY = "parent"


def _f(v):
    try:
        return float(str(v).strip())
    except Exception:  # noqa: BLE001
        return None


def _i(v):
    try:
        return int(float(str(v).strip()))
    except Exception:  # noqa: BLE001
        return None


def sa_parse_university_detail(pp: Dict[str, Any], university_id: int,
                               job_id: Optional[int] = None) -> Dict[str, Any]:
    """Parse a university page's pageProps.response into
    {fields, rankings, costs, nearby, courses, exam_scores}."""
    r = (pp or {}).get("response") or {}
    head = ((r.get("basic_info") or {}).get("head")) or {}
    contact = r.get("contact_location") or {}
    fac = r.get("faculty_stats") or {}
    rating = r.get("college_rating") or r.get("review_stats") or {}
    sub = rating.get("rating") or {}
    now = time.time()

    overview = {str(o.get("label", "")).lower(): o.get("value")
                for o in (r.get("overview_data") or []) if isinstance(o, dict)}

    fields = {
        "name": head.get("college") or "",
        "short_name": head.get("college_short") or "",
        "city": head.get("city") or "",
        "city_id": _i(head.get("city_id")),
        "state": head.get("state") or "",
        "state_id": _i(head.get("state_id")),
        "country_id": _i(head.get("country_id")),
        "country_name": head.get("country") or r.get("country_name") or "",
        "established_year": _i(head.get("establish_year")),
        "institution_type": head.get("institution_type") or "",
        "school_type": head.get("school_type") or "",
        "admin_rating": _f(head.get("admin_rating")),
        "cover_image": head.get("cover_image") or "",
        "website": contact.get("website") or "",
        "address": contact.get("address") or "",
        "email": contact.get("college_mail_id") or "",
        "phone": contact.get("contact") or "",
        "toll_free": contact.get("toll_free_number") or "",
        "latitude": contact.get("latitude") or "",
        "longitude": contact.get("longitude") or "",
        "total_students": _i(head.get("total_students")),
        "total_faculty": _i(fac.get("total_faculty")),
        "faculty_full_time": _i(fac.get("full_time")),
        "faculty_part_time": _i(fac.get("part_time")),
        "graduate_assistants": _i(fac.get("graduate_assistants")),
        "avg_rating": _f(rating.get("average_rating")),
        "total_reviews": _i(rating.get("total_reviews")),
        "rating_academic": _f(sub.get("avg_academic")),
        "rating_accommodation": _f(sub.get("avg_accomodation")),
        "rating_extracurricular": _f(sub.get("avg_extracurricular")),
        "rating_faculty": _f(sub.get("avg_faculty")),
        "rating_infrastructure": _f(sub.get("avg_infrastructure")),
        "rating_placement": _f(sub.get("avg_placement")),
        "attendance_cost": str((r.get("tuition_fees") or {}).get("attendance_cost") or ""),
        "student_faculty_ratio": str(overview.get("student : faculty ratio") or ""),
        "description": str((r.get("article") or {}).get("description") or ""),
        "scholarship_count": len(r.get("scholarships") or []),
        "course_count": len(r.get("important_dates") or []),
    }

    # ---- rankings: agencies x years x streams -----------------------------
    agencies = (r.get("ranking_data") or {}).get("agencies") or {}
    ywd = (r.get("ranking_data") or {}).get("year_wise_data") or {}
    rankings: List[Dict[str, Any]] = []
    for _year_key, entries in (ywd.items() if isinstance(ywd, dict) else []):
        for e in (entries if isinstance(entries, list) else [entries]):
            if not isinstance(e, dict):
                continue
            aid = _i(e.get("agencyId"))
            yr = _i(e.get("year"))
            streams = ((e.get("stream") or {}).get(UNIV_RANK_STREAM_KEY)) or {}
            for sname, sv in (streams.items() if isinstance(streams, dict) else []):
                if not isinstance(sv, dict):
                    continue
                rankings.append({
                    "university_id": university_id, "agency_id": aid,
                    "agency": str((agencies.get(str(aid)) or {}).get("name") or ""),
                    "year": yr, "stream": str(sname),
                    "scope": str(sv.get("scope") or ""),
                    "rank": _i(sv.get("rank")),
                    "rank_out_of": _i(sv.get("rank_out_of")),
                    "country_rank": _i(sv.get("country_rank")),
                    "country_rank_out_of": _i(sv.get("country_rank_out_of")),
                    "score": str(sv.get("score") or ""),
                    "score_out_of": str(sv.get("score_out_of") or ""),
                    "scraped_at": now, "source_job_id": job_id})

    # ---- cost of living ---------------------------------------------------
    costs: List[Dict[str, Any]] = []
    for kind, items in (r.get("accommodation_data") or {}).items():
        for it in (items or []):
            if isinstance(it, dict) and it.get("label"):
                costs.append({"university_id": university_id, "kind": str(kind),
                              "label": str(it["label"]), "value": str(it.get("value") or ""),
                              "scraped_at": now, "source_job_id": job_id})
    for label, value in overview.items():
        costs.append({"university_id": university_id, "kind": "overview",
                      "label": label, "value": str(value),
                      "scraped_at": now, "source_job_id": job_id})

    # ---- nearby -----------------------------------------------------------
    nearby: List[Dict[str, Any]] = []
    for cat, items in (r.get("nearby") or {}).items():
        for it in (items or []):
            if isinstance(it, dict) and it.get("name"):
                nearby.append({"university_id": university_id, "category": str(cat),
                               "name": str(it["name"]), "distance_km": _f(it.get("distance")),
                               "lat": str(it.get("lat") or ""), "lng": str(it.get("lng") or ""),
                               "scraped_at": now, "source_job_id": job_id})

    # ---- per-course block + exam scores -----------------------------------
    courses: List[Dict[str, Any]] = []
    exam_scores: List[Dict[str, Any]] = []
    for c in (r.get("important_dates") or []):
        if not isinstance(c, dict):
            continue
        cid = _i(c.get("course_id"))
        if cid is None:
            continue
        courses.append({
            "university_id": university_id, "course_id": cid,
            "head_one": str(c.get("head_one") or ""),
            "head_two": str(c.get("head_two") or ""),
            "head_short_form": str(c.get("head_short_form") or ""),
            "degree_type": str(c.get("degree_type") or ""),
            "course_duration": str(c.get("course_duration") or ""),
            "course_duration_value": str(c.get("course_duration_value") or ""),
            "total_fee_per_year": str(c.get("total_fee_per_year") or ""),
            "default_fee_per_year": str(c.get("default_fee_per_year") or ""),
            "fee_history_json": json.dumps(c.get("previous_year_course_fees_data") or {},
                                           ensure_ascii=False),
            "application_json": json.dumps(c.get("application") or [], ensure_ascii=False),
            "application_cost": str(c.get("application_cost") or ""),
            "short_entry_reqd": str(c.get("short_entry_reqd") or ""),
            "available_campus": str(c.get("available_campus") or ""),
            "program_url": abs_url(c.get("program_url")) if c.get("program_url") else "",
            "scraped_at": now, "source_job_id": job_id})
        for e in (c.get("exam_data") or []):
            if isinstance(e, dict) and e.get("short_form"):
                exam_scores.append({
                    "program_id": cid, "short_form": str(e["short_form"]),
                    "exam_score": str(e.get("exam_score") or ""),
                    "out_of": str(e.get("exam_out_of_score") or "")})

    return {"fields": fields, "rankings": rankings, "costs": costs,
            "nearby": nearby, "courses": courses, "exam_scores": exam_scores}


def run_university_detail(job_id: int, cfg: Dict[str, Any],
                          log: Callable[[str], None]) -> None:
    """Phase ④: fetch each university's own page. One request per university."""
    import queue as _q
    merged = {**shared_proxy_cfg(), **cfg}
    pm = ProxyManager.from_config(merged)
    stats = Stats()
    adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                             enabled=bool(merged.get("adaptive", True)))
    concurrency = max(1, int(merged.get("concurrency", 4)))
    delay = float(merged.get("delay", 1.0))
    budget_requests = int(merged.get("budget_requests", 0))
    budget_bytes = int(float(merged.get("budget_mb", 0)) * 1024 * 1024)
    keep_desc = bool(merged.get("keep_description", True))

    pending = sa_db.universities_pending_detail(limit=int(merged.get("max_units", 0)))
    total = len(pending)
    sa_db.update_job(job_id, status="running", total_units=total,
                     message=f"{total:,} universities to enrich")
    log(f"SA Phase ④ — university detail: {total:,} universities, "
        f"concurrency={concurrency}")
    if not total:
        sa_db.update_job(job_id, status="completed", finished_at=time.time(),
                         message="nothing pending — run ② Programs first")
        log("nothing pending. Run ② Programs first so universities exist.")
        return

    q: "_q.Queue" = _q.Queue()
    for u in pending:
        q.put(u)
    stop = threading.Event()
    lock = threading.Lock()
    st = {"done": 0, "ranks": 0, "courses": 0, "exams": 0, "err": 0}
    _last_push = {"t": 0.0}

    def budget_hit():
        reqs, byts, _ = stats.snapshot()
        if budget_requests and reqs >= budget_requests:
            return f"request budget reached ({reqs})"
        if budget_bytes and byts >= budget_bytes:
            return f"bandwidth budget reached ({byts/1048576:.1f} MB)"
        return None

    def push():
        reqs, byts, _ = stats.snapshot()
        with lock:
            d, rk, co, ex, er = (st["done"], st["ranks"], st["courses"],
                                 st["exams"], st["err"])
        sa_db.update_job(job_id, done_units=d, items_written=rk + co,
                         req_count=reqs, bytes_count=byts,
                         message=f"{d:,}/{total:,} universities · {rk:,} rankings · "
                                 f"{co:,} course rows · {ex:,} exam scores filled · "
                                 f"{byts/1048576:.1f} MB"
                                 + (f" · {er} errors" if er else ""))

    def worker(idx: int):
        client = Client(pm, log=log, max_retries=int(merged.get("max_retries", 5)),
                        backoff=float(merged.get("backoff", 4)), stats=stats,
                        adaptive=adaptive)
        while not stop.is_set():
            try:
                u = q.get_nowait()
            except _q.Empty:
                return
            uid = int(u["university_id"])
            if sa_db.stop_requested(job_id):
                stop.set(); return
            bh = budget_hit()
            if bh:
                log(f"  ⏸ {bh}"); stop.set(); return
            client.session_id = f"sau{idx}_{uid}"
            try:
                html = client.get_text(u["university_url"])
                pp = _nextdata_pageprops(html)
                parsed = sa_parse_university_detail(pp, uid, job_id)
                if not parsed["fields"].get("name") and not parsed["courses"]:
                    raise ValueError("no basic_info on page")
                f = dict(parsed["fields"])
                if not keep_desc:
                    f["description"] = ""
                sa_db.update_university_detail(uid, f)
                if parsed["rankings"]:
                    sa_db.upsert_university_rankings(parsed["rankings"])
                if parsed["costs"]:
                    sa_db.upsert_university_costs(parsed["costs"])
                if parsed["nearby"]:
                    sa_db.upsert_university_nearby(parsed["nearby"])
                if parsed["courses"]:
                    sa_db.upsert_university_courses(parsed["courses"])
                filled = (sa_db.fill_program_exam_scores(parsed["exam_scores"])
                          if parsed["exam_scores"] else 0)
                sa_db.set_university_progress(uid, "done", len(parsed["courses"]),
                                              len(parsed["rankings"]))
                with lock:
                    st["ranks"] += len(parsed["rankings"])
                    st["courses"] += len(parsed["courses"])
                    st["exams"] += filled
            except Exception as err:  # noqa: BLE001
                # left NOT 'done', so the self-draining queue retries it later
                sa_db.set_university_progress(uid, "error")
                with lock:
                    st["err"] += 1
                log(f"  ! university {uid} failed: {str(err)[:140]}")
            with lock:
                st["done"] += 1
                _d = st["done"]
            _now = time.time()
            if _now - _last_push["t"] >= 5 or _d >= total:
                _last_push["t"] = _now
                push()
            if delay:
                time.sleep(adaptive.value() if adaptive else delay)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    push()
    c = sa_db.counts()
    left = len(sa_db.universities_pending_detail())
    msg = (f"enriched {c.get('universities_detailed', 0):,}/{c.get('universities', 0):,} "
           f"universities · {c.get('university_rankings', 0):,} rankings · "
           f"{c.get('university_courses', 0):,} course rows · "
           f"{st['exams']:,} exam scores filled"
           + (f" · {left:,} still pending" if left else ""))
    sa_db.update_job(job_id, status="completed", message=msg, finished_at=time.time())
    log(msg)
