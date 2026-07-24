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
import time
from typing import Any, Callable, Dict, List, Optional

import sa_db
# Generic infra reused from the domestic engine (imported, never modified):
from scraper import (Client, ProxyManager, encode_payload, abs_url,
                     _to_int, _nextdata_pageprops, BudgetExceeded)

SA_LISTING_API = "https://collegedunia.com/web-api/listing-cf-sa"
SITE = "https://collegedunia.com"
COURSE_FINDER_URL = f"{SITE}/study-abroad/course-finder"
IMG_BASE = "https://image-static.collegedunia.com/public/college_data/images/studyabroad/logos/"
PAGE_SIZE = 20
RESULT_CAP = 10000                    # observed per-query cap (~page 500)
MAX_PAGES = RESULT_CAP // PAGE_SIZE    # 500 — hard guard
# Dimensions used to recursively split a query until each slice is under the cap.
SPLIT_ORDER = ["country", "course_type", "stream", "head_short_form"]


class StopRequested(Exception):
    pass


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


def _build_client(cfg: Dict[str, Any], log) -> "Client":
    pm = ProxyManager.from_config(cfg)
    return Client(pm, log=log, max_retries=int(cfg.get("max_retries", 5)),
                  backoff=float(cfg.get("backoff", 4)))


# ---------------------------------------------------------------------------
# Facets (partition dimensions) — from the SSR __NEXT_DATA__
# ---------------------------------------------------------------------------
def run_facets(job_id: int, cfg: Dict[str, Any], log: Callable[[str], None]) -> None:
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
    client = _build_client(cfg, log)
    _ensure_facets(client, job_id, log)
    delay = float(cfg.get("delay", 1.0))
    budget_bytes = int(float(cfg.get("budget_mb", 0)) * 1024 * 1024)
    done = sa_db.done_partitions()
    state = {"written": 0, "partitions": 0}

    sa_db.update_job(job_id, status="running", message="starting programs crawl")

    def budget_hit() -> bool:
        return bool(budget_bytes and client.stats.snapshot()[1] >= budget_bytes)

    def write_page(data, job_id):
        courses = data.get("courses") or []
        if not courses:
            return 0
        progs = [sa_parse_program(c, job_id) for c in courses]
        sa_db.upsert_programs(progs)
        sa_db.upsert_universities([sa_derive_university(c, job_id) for c in courses])
        exams = []
        for c in courses:
            exams += sa_derive_exams(c)
        sa_db.upsert_program_exams(exams)
        # countries (dedupe by code)
        seen = {}
        for p in progs:
            cc = p.get("country_code")
            if cc and cc not in seen:
                seen[cc] = {"country_code": cc, "country_id": None,
                            "name": cc.upper(), "raw_json": "",
                            "scraped_at": time.time(), "source_job_id": job_id}
        sa_db.upsert_countries(list(seen.values()))
        return len(progs)

    def page_partition(filters, key, first_data):
        prog = sa_db.get_progress(key) or {}
        start = 1
        if prog.get("status") == "partial" and prog.get("last_page"):
            start = int(prog["last_page"]) + 1
        found = int(prog.get("found") or 0)
        if start == 1:
            found += write_page(first_data, job_id)
            sa_db.set_progress(key, "partial", 1, found)
        data = first_data
        page = max(2, start)
        while page <= MAX_PAGES:
            if sa_db.stop_requested(job_id):
                raise StopRequested()
            if budget_hit():
                raise BudgetExceeded("bandwidth budget reached")
            if start > 1 and page == start:
                pass  # resuming; fall through to fetch
            elif not data.get("hasNext", False):
                break
            time.sleep(delay)
            data = fetch_listing(client, filters, page)
            n = write_page(data, job_id)
            found += n
            sa_db.set_progress(key, "partial", page, found)
            state["written"] += n
            sa_db.update_job(job_id, done_units=state["written"], items_written=state["written"],
                             message=f"{key} p{page}: +{n} (total {state['written']})")
            log(f"  {key} p{page}: +{n} (partition total {found})")
            if not data.get("hasNext", False):
                break
            page += 1
        sa_db.set_progress(key, "done", page, found)
        state["partitions"] += 1

    def crawl(filters, di):
        if sa_db.stop_requested(job_id):
            raise StopRequested()
        key = _pkey(filters)
        if key in done:
            log(f"skip (done): {key}")
            return
        first = fetch_listing(client, filters, 1)
        cnt = int(first.get("count") or 0)
        log(f"partition {key} -> count {cnt:,}")
        if cnt == 0:
            sa_db.set_progress(key, "done", 1, 0)
            return
        if cnt <= RESULT_CAP or di >= len(SPLIT_ORDER):
            if cnt > RESULT_CAP:
                log(f"  ! {key} has {cnt:,} > cap {RESULT_CAP:,} and no more split dims — "
                    f"will capture first ~{RESULT_CAP:,} only.")
            page_partition(filters, key, first)
        else:
            dim = SPLIT_ORDER[di]
            vals = _split_values(dim)
            if not vals:
                page_partition(filters, key, first)  # can't split, page what we can
                return
            log(f"  splitting {key} by {dim} ({len(vals)} values)")
            for v in vals:
                crawl({**filters, dim: v}, di + 1)

    try:
        crawl({}, 0)
        sa_db.update_job(job_id, status="completed", finished_at=time.time(),
                         message=f"done: {state['written']} programs, {state['partitions']} partitions")
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
