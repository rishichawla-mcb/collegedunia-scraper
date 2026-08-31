"""
Course Finder — scraper. Self-contained: writes ONLY cf_ tables.

Two phases against https://collegedunia.com/web-api/listing-cf (the private JSON
API behind collegedunia.com/course-finder):

  A. catalogue   the full course list (~21,689). The listing caps at ~1,700
                 results per query, so it is sliced by course_tag_id (200 values,
                 discovered from the page's own filter facets). Each course row
                 also carries `colleges_data.count` — the number of colleges
                 offering it — FREE, with no extra request. That number is what
                 makes phase B costable and orderable in advance.

  B. offerings   for each course, /course-finder?course_id=<id> paginated at 10
                 rows a page, giving each college with fees, ranking, cutoff,
                 admission dates, rating and review count.

Both phases are concurrent (worker pool, one sticky proxy session per worker)
and resumable (partition/course progress tables are self-draining queues).

Reuses the shared HTTP client from `scraper` — proxy rotation, block detection,
exponential backoff and Retry-After all come along for free.
"""
from __future__ import annotations

BUILD = "2026-08-29a"

import json
import queue as _queue
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import cf_db
import db as _core
from scraper import (AdaptiveDelay, Client, ProxyManager, Stats, abs_url)

API_URL = "https://collegedunia.com/web-api/listing-cf"
COURSE_FINDER_URL = "https://collegedunia.com/course-finder"
PAGE_SIZE = 10                     # the API's fixed page size
LISTING_CAP = 1700                 # results reachable in one unfiltered query

_NEXTDATA_RE = re.compile(
    r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def _to_int(v):
    try:
        return int(float(str(v).strip()))
    except Exception:  # noqa: BLE001
        return None


def _to_float(v):
    try:
        return float(str(v).strip())
    except Exception:  # noqa: BLE001
        return None


def _join(v):
    if isinstance(v, list):
        return ", ".join(str(x) for x in v if x)
    return str(v or "")


def _nextdata_pageprops(html: str) -> Dict[str, Any]:
    m = _NEXTDATA_RE.search(html or "")
    if not m:
        return {}
    try:
        d = json.loads(m.group(1))
    except Exception:  # noqa: BLE001
        return {}
    props = d.get("props") or {}
    return (props.get("initialProps") or {}).get("pageProps") or props.get("pageProps") or {}


def parse_course(c: Dict[str, Any], job_id: int = 0) -> Dict[str, Any]:
    """One row of the course-finder listing -> a cf_courses row."""
    lp = c.get("lead_params") or {}
    exam = c.get("exam") if isinstance(c.get("exam"), dict) else {}
    cdata = c.get("colleges_data") if isinstance(c.get("colleges_data"), dict) else {}
    return {
        "course_id": _to_int(lp.get("course_id")),
        "name": str(c.get("name") or ""),
        "course_link": abs_url(c.get("course_link")) if c.get("course_link") else "",
        "listing_link": abs_url(cdata.get("link")) if cdata.get("link") else "",
        "description": str(c.get("description") or ""),
        "eligibility": str(c.get("eligibility") or ""),
        "duration": str(c.get("duration") or ""),
        "level": str(c.get("level") or ""),
        "course_type": str(c.get("course_type") or ""),
        "course_could_be": str(c.get("courses_could_be") or c.get("course_could_be") or ""),
        "degree_could_be": str(c.get("degree_could_be") or ""),
        "fees": str(c.get("fees") or ""),
        "avg_salary": str(c.get("avg_salary") or ""),
        "exam_name": str(exam.get("name") or ""),
        "exam_url": abs_url(exam.get("url")) if exam.get("url") else "",
        "job_roles": _join(c.get("job_roles")),
        "topics_covered": _join(c.get("topics_covered")),
        "stream_id": str(lp.get("stream_id") or ""),
        "course_tag": str(lp.get("course_tag") or ""),
        "course_tag_id": str(lp.get("course_tag_id") or ""),
        "colleges_count": _to_int(cdata.get("count")),
        "colleges_link": abs_url(cdata.get("link")) if cdata.get("link") else "",
        "raw_json": json.dumps(c, ensure_ascii=False),
        "scraped_at": time.time(),
        "source_job_id": job_id,
    }


def parse_offering(c: Dict[str, Any], course_id: int, job_id: int = 0) -> Dict[str, Any]:
    """One row of /course-finder?course_id=<id> -> a cf_offerings row."""
    lp = c.get("lead_params") or {}
    col = c.get("college") if isinstance(c.get("college"), dict) else {}
    exam = c.get("exam") if isinstance(c.get("exam"), dict) else {}
    fee = c.get("fees_data") if isinstance(c.get("fees_data"), dict) else {}
    rank = c.get("ranking_data") if isinstance(c.get("ranking_data"), dict) else {}
    cut = c.get("cutoff") if isinstance(c.get("cutoff"), dict) else {}
    adm = c.get("admission") if isinstance(c.get("admission"), dict) else {}
    return {
        "course_id": course_id,
        "college_id": _to_int(lp.get("college_id")),
        "course_name": str(c.get("name") or ""),
        "college_name": str(col.get("name") or lp.get("college_name") or ""),
        "college_short": str(col.get("short_form") or ""),
        "city": str(col.get("city") or ""),
        "state_id": _to_int(col.get("state_id")),
        "college_link": abs_url(col.get("link")) if col.get("link") else "",
        "logo": str(col.get("logo") or ""),
        "offering_link": abs_url(c.get("link")) if c.get("link") else "",
        "fees_amount": _to_int(fee.get("amount")),
        "fees_text": str(fee.get("text") or ""),
        "eligibility": str(c.get("eligibility") or ""),
        "duration": str(c.get("duration") or ""),
        "level": str(c.get("level") or ""),
        "course_type": str(c.get("course_type") or ""),
        "course_could_be": str(c.get("course_could_be") or ""),
        "degree_could_be": str(c.get("degree_could_be") or ""),
        "exam_name": str(exam.get("name") or ""),
        "exam_url": abs_url(exam.get("url")) if exam.get("url") else "",
        "ranking_agency": str(rank.get("agency") or ""),
        "ranking_rank": _to_int(rank.get("rank")),
        "ranking_total": _to_int(rank.get("total")),
        "ranking_stream": str(rank.get("stream") or ""),
        "ranking_url": abs_url(rank.get("url")) if rank.get("url") else "",
        "cutoff_exam": str(cut.get("exam_name") or ""),
        "cutoff_value": _to_float(cut.get("cutoff")),
        "admission_start": str(adm.get("admission_start_date") or ""),
        "admission_end": str(adm.get("admission_end_date") or ""),
        "course_rating": _to_float(c.get("course_rating")),
        "reviews_count": _to_int(c.get("reviews_count")),
        "avg_salary": str(c.get("avg_salary") or ""),
        "job_roles": _join(c.get("job_roles")),
        "major_stream_rating": _to_float(lp.get("major_stream_rating")),
        "stream_id": str(lp.get("stream_id") or ""),
        "course_tag": str(lp.get("course_tag") or ""),
        "course_tag_id": str(lp.get("course_tag_id") or ""),
        "raw_json": json.dumps(c, ensure_ascii=False) if _keep_raw() else None,
        "scraped_at": time.time(),
        "source_job_id": job_id,
    }


_KEEP_RAW = {"v": True}


def _keep_raw() -> bool:
    return _KEEP_RAW["v"]


# ---------------------------------------------------------------------------
# Shared plumbing
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
            "delay": float(g("delay", 1.0) or 1.0),
        }
    except Exception:  # noqa: BLE001
        return {"proxy_mode": "none", "proxy_gateway": "", "proxy_list": [],
                "proxy_cooldown": 120, "delay": 1.0}


def _client(pm, cfg, log, stats, adaptive) -> Client:
    return Client(pm, log=log,
                  max_retries=int(cfg.get("max_retries", 5)),
                  backoff=float(cfg.get("backoff", 4)),
                  stats=stats, adaptive=adaptive)


def _fetch(client: Client, payload: Dict[str, Any]) -> Dict[str, Any]:
    """One listing-cf call. Reuses Client.fetch so proxy rotation, 403 handling,
    exponential backoff and Retry-After all apply."""
    return client.fetch(payload)


def _facets(client: Client, log) -> Dict[str, List[str]]:
    """Read the course-finder filter facets from the page's own SSR payload."""
    html = client.get_text(COURSE_FINDER_URL)
    pp = _nextdata_pageprops(html)
    filters = ((pp.get("filterResponse") or {}).get("filters")) or {}
    out: Dict[str, List[str]] = {}
    for fname, fobj in filters.items():
        vals = [str(v.get("value")) for v in (fobj.get("values") or [])
                if v.get("value") not in (None, "")]
        if vals:
            out[fname] = vals
    total = (pp.get("listingResponse") or {}).get("count")
    log(f"  facets: {', '.join(f'{k}={len(v)}' for k, v in sorted(out.items()))}"
        f" · catalogue total ≈ {total:,}" if total else "")
    return out


# ---------------------------------------------------------------------------
# Phase A — the course catalogue
# ---------------------------------------------------------------------------
def run_catalogue(job_id: int, cfg: Dict[str, Any],
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

    cf_db.update_job(job_id, status="running", message="reading filter facets…")
    log(f"Course Finder · Phase A [BUILD {BUILD}] concurrency={concurrency}")

    boot = _client(pm, merged, log, stats, adaptive)
    boot.session_id = f"cfboot{int(time.time())}"
    try:
        facets = _facets(boot, log)
    except Exception as err:  # noqa: BLE001
        log(f"  ! facet fetch failed ({err}); falling back to an unsliced sweep")
        facets = {}
    cf_db.set_setting("facets", facets)

    # Slice by course_tag_id — 200 values, each comfortably under the ~1,700 cap.
    dim = str(merged.get("partition_by", "course_tag_id"))
    values = facets.get(dim) or []
    if not values:
        partitions = [("ALL", {})]
        log("  no facet values — single unsliced partition (will hit the ~1,700 cap)")
    else:
        partitions = [(f"{dim}={v}", {dim: v}) for v in values]
    if not merged.get("force_restart"):
        done = cf_db.done_partitions()
        skipped = len([p for p, _ in partitions if p in done])
        partitions = [(k, f) for k, f in partitions if k not in done]
        if skipped:
            log(f"  resuming: {skipped} partitions already done")

    total = len(partitions)
    cf_db.update_job(job_id, total_units=total,
                     message=f"{total} partitions to sweep")
    log(f"  {total} partitions to sweep by {dim}")

    q: "_queue.Queue" = _queue.Queue()
    for p in partitions:
        q.put(p)

    stop = threading.Event()
    lock = threading.Lock()
    state = {"done": 0, "rows": 0}

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
        cf_db.update_job(job_id, done_units=d, items_written=rw, req_count=reqs,
                         bytes_count=byts,
                         message=f"{d}/{total} partitions · {rw:,} courses · "
                                 f"{byts/1048576:.1f} MB")

    def worker(idx: int):
        client = _client(pm, merged, log, stats, adaptive)
        while not stop.is_set():
            try:
                key, filt = q.get_nowait()
            except _queue.Empty:
                return
            if cf_db.stop_requested(job_id):
                stop.set()
                return
            bh = budget_hit()
            if bh:
                log(f"  ⏸ {bh}")
                stop.set()
                return
            # one sticky IP per partition so pagination stays on a single exit node
            client.session_id = f"cf{idx}_{abs(hash(key)) % 10**8}"
            page, found = 1, 0
            try:
                while not stop.is_set():
                    data = _fetch(client, {**filt, "page": page})
                    rows = data.get("courses") or []
                    if not rows:
                        break
                    parsed = [parse_course(c, job_id) for c in rows]
                    parsed = [p for p in parsed if p["course_id"] is not None]
                    if parsed:
                        cf_db.upsert_courses(parsed)
                    found += len(parsed)
                    with lock:
                        state["rows"] += len(parsed)
                    cf_db.set_partition(key, "partial", page, found)
                    if not data.get("hasNext"):
                        break
                    if page * PAGE_SIZE >= LISTING_CAP:
                        log(f"  ! {key} hit the {LISTING_CAP}-result ceiling — "
                            f"slice further to reach the rest")
                        break
                    page += 1
                    if delay:
                        time.sleep(adaptive.value() if adaptive else delay)
                cf_db.set_partition(key, "done", page, found)
            except Exception as err:  # noqa: BLE001
                log(f"  ! partition {key} failed at page {page}: {err}")
                cf_db.set_partition(key, "partial", page, found)
            with lock:
                state["done"] += 1
            push()

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    push()
    c = cf_db.counts()
    fc = cf_db.phase_b_forecast()
    msg = (f"catalogue: {c['courses']:,} courses "
           f"({c['courses_with_count']:,} with a college count) · "
           f"phase B forecast: {fc['expected_offerings']:,} offerings "
           f"in ~{fc['expected_pages']:,} requests")
    cf_db.update_job(job_id, status="completed", message=msg,
                     finished_at=time.time())
    log(msg)


# ---------------------------------------------------------------------------
# Phase B — colleges offering each course
# ---------------------------------------------------------------------------
def run_offerings(job_id: int, cfg: Dict[str, Any],
                  log: Optional[Callable[[str], None]] = None) -> None:
    log = log or (lambda m: print(m, flush=True))
    merged = {**_proxy_cfg(), **cfg}
    _KEEP_RAW["v"] = bool(merged.get("keep_raw", True))
    pm = ProxyManager.from_config(merged)
    stats = Stats()
    adaptive = AdaptiveDelay(float(merged.get("delay", 1.0)),
                             enabled=bool(merged.get("adaptive", True)))
    concurrency = max(1, int(merged.get("concurrency", 8)))
    delay = float(merged.get("delay", 1.0))
    budget_requests = int(merged.get("budget_requests", 0))
    budget_bytes = int(float(merged.get("budget_mb", 0)) * 1024 * 1024)
    max_courses = int(merged.get("max_courses", 0))
    min_colleges = int(merged.get("min_colleges", 0))
    order = str(merged.get("order", "value"))

    pending = cf_db.courses_pending(limit=max_courses, min_colleges=min_colleges,
                                    order=order)
    total = len(pending)
    exp_pages = sum(max(1, ((p.get("colleges_count") or 0) + PAGE_SIZE - 1) // PAGE_SIZE)
                    for p in pending)
    exp_rows = sum(p.get("colleges_count") or 0 for p in pending)
    cf_db.update_job(job_id, status="running", total_units=total,
                     message=f"{total:,} courses queued · ~{exp_rows:,} offerings "
                             f"in ~{exp_pages:,} requests")
    log(f"Course Finder · Phase B [BUILD {BUILD}] {total:,} courses, "
        f"concurrency={concurrency}, ~{exp_pages:,} requests expected"
        + (" (raw_json OFF)" if not _keep_raw() else ""))
    if not total:
        cf_db.update_job(job_id, status="completed",
                         message="nothing pending — run Phase A first",
                         finished_at=time.time())
        log("nothing pending. Run Phase A (catalogue) first.")
        return

    q: "_queue.Queue" = _queue.Queue()
    for p in pending:
        q.put(p)

    stop = threading.Event()
    lock = threading.Lock()
    state = {"done": 0, "rows": 0, "empty": 0, "err": 0}
    _last_push = {"t": 0.0}

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
            d, rw, er = state["done"], state["rows"], state["err"]
        cf_db.update_job(job_id, done_units=d, items_written=rw, req_count=reqs,
                         bytes_count=byts,
                         message=f"{d:,}/{total:,} courses · {rw:,} offerings · "
                                 f"{byts/1048576:.1f} MB"
                                 + (f" · {er} errors" if er else ""))

    def worker(idx: int):
        client = _client(pm, merged, log, stats, adaptive)
        while not stop.is_set():
            try:
                row = q.get_nowait()
            except _queue.Empty:
                return
            cid = int(row["course_id"])
            expected = int(row.get("colleges_count") or 0)
            if cf_db.stop_requested(job_id):
                stop.set()
                return
            bh = budget_hit()
            if bh:
                log(f"  ⏸ {bh}")
                stop.set()
                return
            client.session_id = f"cfb{idx}_{cid}"   # sticky IP for this course's pages
            page, found = 1, 0
            try:
                while not stop.is_set():
                    data = _fetch(client, {"course_id": str(cid), "page": page})
                    rows = data.get("courses") or []
                    if not rows:
                        break
                    parsed = [parse_offering(c, cid, job_id) for c in rows]
                    parsed = [p for p in parsed if p["college_id"] is not None]
                    if parsed:
                        cf_db.upsert_offerings(parsed)
                    found += len(parsed)
                    with lock:
                        state["rows"] += len(parsed)
                    cf_db.set_course_progress(cid, "partial", page, found, expected)
                    if not data.get("hasNext"):
                        break
                    page += 1
                    if delay:
                        time.sleep(adaptive.value() if adaptive else delay)
                status = "done" if found else "empty"
                cf_db.set_course_progress(cid, status, page, found, expected)
                if not found:
                    with lock:
                        state["empty"] += 1
            except Exception as err:  # noqa: BLE001
                # Leave it 'partial' so the self-draining queue retries it later.
                cf_db.set_course_progress(cid, "partial", page, found, expected)
                with lock:
                    state["err"] += 1
                log(f"  ! course {cid} failed at page {page} "
                    f"(kept {found} rows, will resume): {err}")
            with lock:
                state["done"] += 1
                _d = state["done"]
            # Push on a TIME cadence, not every-Nth-course. With N=25 an 84-course
            # run showed "0/84 · 0 rows" for its whole first third, which reads as
            # a hung job. Also always push the last item so the final state lands.
            _now = time.time()
            if _now - _last_push["t"] >= 5 or _d >= total:
                _last_push["t"] = _now
                push()

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(concurrency)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    push()
    c = cf_db.counts()
    fc = cf_db.phase_b_forecast()
    msg = (f"offerings: {c['offerings']:,} rows across "
           f"{c['distinct_colleges']:,} colleges · "
           f"{c['courses_scraped']:,}/{c['courses']:,} courses done"
           + (f" · {fc['courses_left']:,} still pending" if fc["courses_left"] else ""))
    cf_db.update_job(job_id, status="completed", message=msg, finished_at=time.time())
    log(msg)
