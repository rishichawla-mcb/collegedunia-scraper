"""
Shiksha — UI section rendered inside app.py behind the vertical switch.

Reads and writes ONLY the Shiksha database FILE (`sk_db.SK_DB_PATH`), never
`data.db`. Uses the SAME proxy the rest of the app has configured. Scrapes run as
detached worker processes (`platform_worker.py shiksha <job>`).
"""
from __future__ import annotations

BUILD = "2026-09-24a"

import os
import subprocess
import sys
import time

import pandas as pd
import streamlit as st

import db as _core
import sk_db
import sk_scraper
import sk_vertical  # noqa: F401  registers 'shiksha'
_VERTICALS = (sk_vertical,)   # referenced so pyflakes keeps the registration import

# Measured 2026-09-24 on a real college home page: 170 KB on the wire against
# 1,072 KB decompressed. This is the number that decides whether phase Ⓑ is
# affordable at all, so the UI shows it rather than burying it.
KB_PER_COLLEGE = 170.0
FALLBACK_SECS_PER_REQ = 4.0

MAX_DL_MB = float(os.environ.get("CD_MAX_DOWNLOAD_MB", "150"))


# ---------------------------------------------------------------------------
# Download plumbing — identical contract to cf_ui: build to a temp file, refuse
# anything over the cap rather than OOM a 2 GB box.
# ---------------------------------------------------------------------------
def _prepare(build, suffix):
    import tempfile
    fd, tmp = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    try:
        build(tmp)
        size = os.path.getsize(tmp)
        if size > MAX_DL_MB * 1048576:
            return None, size
        with open(tmp, "rb") as fh:
            return fh.read(), size
    except Exception as err:  # noqa: BLE001
        st.error(f"Export failed: {str(err)[:300]}")
        return None, -1
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass


def _offer(container, data, size, fname, key):
    if data is None:
        if size >= 0:
            container.error(
                f"**{fname} would be {size/1048576:,.0f} MB — too big to hand to "
                f"the browser** (cap {MAX_DL_MB:.0f} MB). Untick *include raw "
                f"payload*, or filter to fewer rows.")
        st.session_state.pop(key, None)
        return
    st.session_state[key] = (fname, data, size)


def _render_pending(container, key, mime):
    item = st.session_state.get(key)
    if not item:
        return
    fname, data, size = item
    container.caption(f"Ready: **{fname}** · {size/1048576:.1f} MB")
    container.download_button(f"⬇️ Download {fname}", data=data, file_name=fname,
                              mime=mime, key=f"dl_{key}")
    if container.button("🧹 Clear from memory", key=f"clr_{key}"):
        st.session_state.pop(key, None)
        st.rerun()


def _to_csv(table: str, include_raw: bool, out_path: str) -> None:
    """Stream the table to CSV in chunks — a 57k-row table is small, but
    sk_offerings will not be, and pandas holding the whole frame is what killed
    the domestic export."""
    first = True
    with sk_db.connect() as conn:
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
        if not include_raw:
            cols = [c for c in cols if c != "raw_json"]
        sel = ",".join(cols)
        for chunk in pd.read_sql_query(f"SELECT {sel} FROM {table}", conn,
                                       chunksize=50000):
            chunk.to_csv(out_path, mode="w" if first else "a",
                         header=first, index=False)
            first = False
    if first:                      # empty table — still produce a valid file
        pd.DataFrame(columns=cols).to_csv(out_path, index=False)


def _estimate_mb(table: str, include_raw: bool) -> float:
    try:
        with sk_db.connect() as conn:
            n = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if not n:
                return 0.0
            cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
            sel = ",".join(f"COALESCE(LENGTH(CAST({c} AS TEXT)),0)"
                           for c in cols
                           if include_raw or c != "raw_json") or "0"
            avg = conn.execute(
                f"SELECT AVG({sel}) FROM (SELECT * FROM {table} LIMIT 2000)"
            ).fetchone()[0] or 0
        return n * (float(avg) + len(cols)) / 1048576.0
    except Exception:  # noqa: BLE001
        return 0.0


# ---------------------------------------------------------------------------
def _launch(job_id: int) -> None:
    try:
        subprocess.Popen([sys.executable, "platform_worker.py", "shiksha",
                          str(job_id)],
                         start_new_session=True,
                         cwd=os.path.dirname(os.path.abspath(__file__)))
    except Exception as e:  # noqa: BLE001
        st.error(f"Failed to launch worker: {e}")


def _proxy_line() -> str:
    mode = _core.get_setting("proxy_mode", "none")
    gw = _core.proxy_gateway()
    if mode == "gateway" and gw:
        return f"gateway → `{gw.split('@')[-1]}`"
    if mode == "list":
        n = len([p for p in (_core.get_setting("proxy_list_text", "") or "").splitlines()
                 if p.strip()])
        return f"list ({n} proxies)"
    return "**direct (no proxy)** — Shiksha runs will refuse to start"


def _measured_rate():
    """Requests/second actually achieved by this vertical's completed jobs."""
    try:
        for j in sk_db.list_jobs(limit=25):
            if j.get("status") != "completed" or not (j.get("req_count") or 0):
                continue
            dur = (j.get("finished_at") or 0) - (j.get("started_at") or 0)
            if dur > 60 and j["req_count"] > 20:
                return j["req_count"] / dur, j["id"]
    except Exception:  # noqa: BLE001
        pass
    return None, None


def _fmt_dur(secs: float) -> str:
    if secs < 90:
        return f"~{secs:.0f}s"
    if secs < 5400:
        return f"~{secs/60:.0f} min"
    if secs < 172800:
        return f"~{secs/3600:.1f} h"
    return f"~{secs/86400:.1f} days"


def _fmt_size(nbytes: float) -> str:
    if nbytes < 1024**2:
        return f"{nbytes/1024:,.0f} KB"
    if nbytes < 1024**3:
        return f"{nbytes/1024**2:,.0f} MB"
    return f"{nbytes/1024**3:,.1f} GB"


def _cfg(**extra):
    cfg = {
        "delay": st.session_state.get("delay", 1.0),
        "max_retries": st.session_state.get("max_retries", 5),
        "backoff": st.session_state.get("backoff", 4.0),
        "adaptive": st.session_state.get("adaptive", True),
    }
    cfg.update(extra)
    return cfg


def _job_monitor(key: str) -> None:
    jobs = sk_db.list_jobs(limit=10)
    if not jobs:
        return
    j = jobs[0]
    st.markdown("###### Latest job")
    k = st.columns(5)
    k[0].metric("Job", f"#{j['id']} {j['phase']}")
    k[1].metric("Status", j["status"] or "—")
    tot = j.get("total_units") or 0
    done = j.get("done_units") or 0
    k[2].metric("Progress", f"{done:,}/{tot:,}" if tot else f"{done:,}")
    k[3].metric("Rows", f"{(j.get('items_written') or 0):,}")
    k[4].metric("Bandwidth", f"{(j.get('bytes_count') or 0)/1048576:,.1f} MB")
    st.caption(j.get("message") or "")
    if tot:
        st.progress(min(1.0, done / tot))
    c1, c2 = st.columns(2)
    if j["status"] == "running" and c1.button("⏹️ Stop", key=f"skstop{key}{j['id']}"):
        sk_db.request_stop(j["id"])
        st.warning("Stop requested — the worker finishes its current sitemap and exits.")
    if c2.button("🔄 Refresh", key=f"skref{key}{j['id']}"):
        st.rerun()
    with st.expander("Log", expanded=False):
        st.code("\n".join(r["message"] for r in sk_db.get_logs(j["id"], 200)) or "—")


# ---------------------------------------------------------------------------
def render() -> None:
    sk_db.init_db()
    st.title("📗 Shiksha")
    st.caption(f"shiksha.com · proxy: {_proxy_line()} · own database file "
               f"`{os.path.basename(sk_db.SK_DB_PATH)}` — completely separate from "
               f"the Collegedunia data")

    c = sk_db.counts()
    fc = sk_db.detail_forecast(kb_per_college=KB_PER_COLLEGE)

    m = st.columns(5)
    m[0].metric("Colleges", f"{c['colleges']:,}")
    m[1].metric("Offerings", f"{c['offerings']:,}")
    m[2].metric("Courses", f"{c['courses']:,}")
    m[3].metric("Universities", f"{c['universities']:,}")
    m[4].metric("Sitemaps done", f"{c['sitemaps_done']:,}")

    tab_a, tab_b, tab_c, tab_data, tab_hist = st.tabs(
        ["Ⓐ Discovery", "Ⓑ College detail", "Ⓒ Courses", "Data", "History"])

    # ----------------------------------------------------------------- Ⓐ
    with tab_a:
        st.subheader("Ⓐ Discovery — the whole inventory, from the sitemaps")
        st.caption(
            "Shiksha publishes every college, university and college×course URL "
            "in ~48 gzipped sitemaps, and **the ids are in the URLs**. So this "
            "costs ~35 MB and no college page is fetched. 84,193 college home "
            "URLs resolve to **57,695 distinct ids** — it dedupes by id (the "
            "~26k alias slugs are stored, not discarded), and every "
            "college × course edge comes out free.")

        a1, a2, a3 = st.columns(3)
        conc_a = a1.number_input("Parallel workers", 1, 12, 4, key="ska_conc")
        budget_a = a2.number_input("Bandwidth budget (MB, 0 = none)", 0, 5000, 0,
                                   step=25, key="ska_mb")
        force_a = a3.checkbox("Restart from scratch", value=False, key="ska_force",
                              help="Off = resume, skipping sitemaps already read.")

        _r, _ = _measured_rate()
        st.caption(f"~48 sitemaps ≈ 35 MB → "
                   f"**{_fmt_dur(48 / _r) if _r else _fmt_dur(48 * FALLBACK_SECS_PER_REQ / max(1, int(conc_a)))}**"
                   + (f" (measured {_r:.2f} req/s)" if _r else " (estimate)"))

        if _core.get_setting("proxy_mode", "none") == "none":
            st.warning(
                "No proxy is configured. Shiksha runs refuse to start without one "
                "— it has never been touched from a datacentre IP, and a direct "
                "run would put this server's own IP in front of that question.",
                icon="🔒")

        if st.button("▶️ Run discovery", type="primary", key="ska_run"):
            jid = sk_db.create_job("discovery", _cfg(
                concurrency=int(conc_a), budget_mb=float(budget_a),
                force_restart=bool(force_a)))
            _launch(jid)
            st.success(f"Started discovery — job #{jid}")
            time.sleep(1)
            st.rerun()
        _job_monitor("a")

        with sk_db.connect() as conn:
            smaps = pd.read_sql_query(
                "SELECT sitemap_url, kind, status, urls, colleges, offerings, "
                "universities, bytes, message FROM sk_sitemap_progress "
                "ORDER BY sitemap_url", conn)
        if not smaps.empty:
            smaps["sitemap"] = smaps["sitemap_url"].str.rsplit("/", n=1).str[-1]
            st.markdown("###### Sitemaps")
            st.dataframe(
                smaps[["sitemap", "kind", "status", "urls", "colleges",
                       "offerings", "universities", "bytes", "message"]],
                use_container_width=True, hide_index=True, height=300)

    # ----------------------------------------------------------------- Ⓑ
    with tab_b:
        st.subheader("Ⓑ College detail — attributes and fees")
        st.caption(
            "One request per college home page. The whole dataset is already in "
            "the page (`__PRELOADED_STATE__`, 616 KB of the 1,076 KB body), so "
            "there is no cheaper endpoint — the only `apis.shiksha.com` URL in "
            "the HTML is an error logger. About **2.3 KB per college** survives "
            "parsing, which is what makes 57,751 colleges fit on this disk.")

        f = st.columns(4)
        f[0].metric("Colleges pending", f"{fc['colleges_left']:,}")
        f[1].metric("With detail", f"{c['colleges_with_detail']:,}")
        f[2].metric("Base courses", f"{c['base_courses']:,}")
        f[3].metric("Course rows", f"{c['college_base_courses']:,}")

        if not c["colleges"]:
            st.info("Run discovery first — phase Ⓑ reads its queue from it.")
        else:
            b1, b2, b3 = st.columns(3)
            conc_b = b1.number_input("Parallel workers", 1, 12, 4, key="skb_conc")
            budget_b = b2.number_input("Bandwidth budget (MB, 0 = none)", 0,
                                       20000, 0, step=250, key="skb_mb")
            max_b = b3.number_input("Max colleges (0 = all)", 0, 100000, 0,
                                    step=500, key="skb_max")
            st.caption(
                f"All {fc['colleges_left']:,} pending ≈ **{fc['est_gb_left']:,.1f} GB** "
                f"on the wire at the measured {KB_PER_COLLEGE:.0f} KB each. "
                f"Set a max to take a slice first.")

            st.info(
                "**Route: direct, proxy on refusal.** Shiksha serves this "
                "server's own IP at the same size and speed as the gateway, so "
                "the crawl spends no proxy quota. The first 403/429 switches "
                "every later request onto the proxy permanently, and the job log "
                "records the switch.", icon="🛣️")
            start_direct = st.checkbox(
                "Start direct", value=True, key="skb_direct",
                help="Untick to go through the proxy from the first request.")

            if st.button("▶️ Run college detail", type="primary", key="skb_run"):
                jid = sk_db.create_job("detail", _cfg(
                    concurrency=int(conc_b), budget_mb=float(budget_b),
                    max_colleges=int(max_b), start_direct=bool(start_direct)))
                _launch(jid)
                st.success(f"Started college detail — job #{jid}")
                time.sleep(1)
                st.rerun()
            _job_monitor("b")

        if c["base_courses"]:
            with sk_db.connect() as conn:
                cat = pd.read_sql_query(
                    "SELECT base_course_id, name, level, colleges_count "
                    "FROM sk_base_courses ORDER BY colleges_count DESC LIMIT 25",
                    conn)
            st.markdown("**The shared base-course catalogue** — `baseCourseTuples[].id`, "
                        "which never appears in a URL. This is the join key for the "
                        "Collegedunia comparison.")
            st.dataframe(cat, use_container_width=True, hide_index=True)

    # ----------------------------------------------------------------- Ⓒ/Ⓓ
    with tab_c:
        st.subheader("Ⓒ Course listing — one row per ACTUAL course")
        st.caption(
            "Phase Ⓑ reads `baseCourseTuples`, which are grouped **by base "
            "course**: one *B.Des* row whose fee range covers every B.Des the "
            "college runs. This walks `/courses` and `/courses-2…` — 12 courses "
            "a page, following the site's own `paginationData` — and writes "
            "**fees as a number**, duration, seats, exams, eligibility, median "
            "salary, skills and admission status, per course.")
        st.info(
            "**No API is involved.** Loading `/courses` in a real browser and "
            "scrolling it to the bottom produces zero requests to any api host — "
            "the rows are already in `__PRELOADED_STATE__.childPageData."
            "courseTuples`. The earlier note that the list was 'fetched "
            "client-side after hydration' read `totalCourseCount: 0` as the list "
            "length; it is a filtered counter. Measured live on 2026-09-28.",
            icon="🔎")

        g = st.columns(4)
        left_c = len(sk_db.colleges_pending_courses())
        g[0].metric("Colleges pending", f"{left_c:,}")
        g[1].metric("Colleges listed", f"{c['course_listing_done']:,}")
        g[2].metric("Course rows with fees", f"{c['offerings_with_fees']:,}")
        g[3].metric("of offerings", f"{c['offerings']:,}")

        if not c["colleges_with_detail"]:
            st.info("Run phase Ⓑ first — phase Ⓒ only asks for the courses of a "
                    "college whose home page it has already resolved.")
        else:
            k1, k2, k3 = st.columns(3)
            conc_c = k1.number_input("Parallel workers", 1, 12, 4, key="skc_conc")
            budget_c = k2.number_input("Bandwidth budget (MB, 0 = none)", 0,
                                       40000, 0, step=250, key="skc_mb")
            max_c = k3.number_input("Max colleges (0 = all)", 0, 100000, 0,
                                    step=500, key="skc_max")
            est_pages = max(left_c, int(c["offerings"] / 12) + left_c)
            st.caption(
                f"≈**{est_pages:,} pages** for the {left_c:,} colleges left "
                f"≈ **{est_pages*155/1048576:,.1f} GB** at the measured "
                f"155 KB/page.")
            if st.button("▶️ Run course listing", type="primary", key="skc_run"):
                jid = sk_db.create_job("courses", _cfg(
                    concurrency=int(conc_c), budget_mb=float(budget_c),
                    max_colleges=int(max_c)))
                _launch(jid)
                st.success(f"Started course listing — job #{jid}")
                time.sleep(1)
                st.rerun()
            _job_monitor("c")

        st.divider()
        st.subheader("Ⓓ Course detail — specialization and the fee breakdown")
        st.caption(
            "One request **per course** — 89 top-level keys on the page, of "
            "which 75 are stored. Adds what the listing has not got: "
            "specialization + id, stream/substream ids, level (UG/PG), "
            "credential, education type, delivery method, medium; the full fee "
            "**breakdown** — tuition, one-time payment, hostel, deposit, other, "
            "what the total includes, the fee year, the brochure, and the prose "
            "that qualifies each figure; eligibility including **category-wise "
            "class-XII cutoffs** and per-exam cutoffs; the ordered admission "
            "steps; seats by category/exam/domicile; placements with the "
            "**grain** of the salary figure; recruiters; the affiliating "
            "university; highlights; and dated events.")
        st.info(
            "**Two things worth knowing before you use the numbers.** (1) Every "
            "fee is a *category-keyed* map, but across a private design "
            "college, a private B-school and NIT Trichy only `general` is ever "
            "populated — NIT's own note says that one figure covers "
            "OPEN/OPEN-PWD/OPEN-EWS/OBC-NCL/OBC-PWD, which is why the prose is "
            "stored beside the number. The whole map is kept anyway, so a "
            "college that does publish SC/ST figures will not lose them. "
            "(2) `placement_grain` says what the salary describes — at college "
            "72 it is `substreamId` (the substream, not this course); at NIT "
            "Trichy it is `clientCourse`.", icon="⚖️")
        d = st.columns(3)
        d[0].metric("Courses enriched", f"{c['offerings_deep']:,}")
        d[1].metric("With a specialization", f"{c['offerings_with_spec']:,}")
        d[2].metric("Listed, not yet enriched",
                    f"{max(0, c['offerings_listed'] - c['offerings_deep']):,}")
        st.warning(
            f"At the measured 144 KB each, all {c['offerings']:,} offerings "
            f"would be ≈**{c['offerings']*144/1048576:,.0f} GB**. Use a cap and "
            f"let it drain across several runs.", icon="💸")
        if not c["offerings_listed"]:
            st.info("Run phase Ⓒ first — it is what supplies the per-course URL.")
        else:
            e1, e2, e3 = st.columns(3)
            conc_d = e1.number_input("Parallel workers", 1, 12, 4, key="skd_conc")
            budget_d = e2.number_input("Bandwidth budget (MB, 0 = none)", 0,
                                       60000, 2000, step=250, key="skd_mb")
            max_d = e3.number_input("Max courses (0 = all)", 0, 500000, 5000,
                                    step=1000, key="skd_max")
            if st.button("▶️ Run course detail", key="skdd_run"):
                jid = sk_db.create_job("course_detail", _cfg(
                    concurrency=int(conc_d), budget_mb=float(budget_d),
                    max_courses=int(max_d)))
                _launch(jid)
                st.success(f"Started course detail — job #{jid}")
                time.sleep(1)
                st.rerun()
            _job_monitor("d")

        if c["offerings_with_fees"]:
            with sk_db.connect() as conn:
                sample = pd.read_sql_query(
                    "SELECT college_id, course_id, course_name, base_course_name,"
                    "       specialization, course_level, duration, total_seats,"
                    "       fees_amount, fees_total, fees_onetime, fees_hostel,"
                    "       fees_other, fees_year, elig_xii_general,"
                    "       placement_grain, placement_pct, salary_max,"
                    "       affiliation_name FROM sk_offerings "
                    "WHERE fees_amount IS NOT NULL "
                    "ORDER BY fees_amount DESC LIMIT 25", conn)
            st.markdown("**Per-course rows** — the grain phase Ⓑ could not reach. "
                        "A blank `specialization` means phase Ⓓ has not reached "
                        "that course yet, not that it has none.")
            st.dataframe(sample, use_container_width=True, hide_index=True)

    # -------------------------------------------------------------- Data
    with tab_data:
        st.subheader("Data")
        which = st.selectbox(
            "Table", ["sk_colleges", "sk_offerings", "sk_courses",
                      "sk_universities", "sk_college_aliases",
                      "sk_sitemap_progress", "sk_college_progress",
                      "sk_course_progress"],
            key="skd_tbl")
        with sk_db.connect() as conn:
            n = conn.execute(f"SELECT COUNT(*) FROM {which}").fetchone()[0]
            st.caption(f"{n:,} rows")
            if n:
                df = pd.read_sql_query(f"SELECT * FROM {which} LIMIT 300", conn)
                drop = [c_ for c_ in ("raw_json", "detail_json") if c_ in df.columns]
                st.dataframe(df.drop(columns=drop), use_container_width=True,
                             height=420)

        if which == "sk_courses" and n:
            with sk_db.connect() as conn:
                top = pd.read_sql_query(
                    "SELECT name, slug, colleges_count FROM sk_courses "
                    "WHERE COALESCE(colleges_count,0)>0 "
                    "ORDER BY colleges_count DESC LIMIT 20", conn)
            if not top.empty:
                st.markdown("**Biggest courses by college count**")
                st.dataframe(top, use_container_width=True, hide_index=True)

        if which == "sk_colleges" and n:
            with sk_db.connect() as conn:
                al = pd.read_sql_query(
                    "SELECT college_id, slug, alias_count, tabs FROM sk_colleges "
                    "WHERE COALESCE(alias_count,0)>1 "
                    "ORDER BY alias_count DESC LIMIT 20", conn)
            if not al.empty:
                st.markdown("**Most aliased colleges** — several URLs, one id. "
                            "Crawling by URL instead of by id would fetch each of "
                            "these repeatedly.")
                st.dataframe(al, use_container_width=True, hide_index=True)

        st.divider()
        st.markdown("### ⬇️ Export")
        raw = st.checkbox("Include raw payload (`raw_json`)", value=False,
                          key="ske_raw")
        st.caption(f"Built to a temp file first; anything over {MAX_DL_MB:.0f} MB "
                   f"is refused rather than served.")
        st.caption(f"`{which}` ≈ {_estimate_mb(which, raw):,.1f} MB")
        if st.button(f"🛠️ Build {which}.csv", key="ske_csv"):
            with st.spinner("Building CSV…"):
                d, s = _prepare(lambda p: _to_csv(which, raw, p), ".csv")
            _offer(st, d, s, f"{which}.csv", "ske_csv_data")
        _render_pending(st, "ske_csv_data", "text/csv")

    # ----------------------------------------------------------- History
    with tab_hist:
        st.subheader("History")
        jobs = sk_db.list_jobs(limit=50)
        if jobs:
            df = pd.DataFrame(jobs)
            cols = [c_ for c_ in ("id", "phase", "status", "done_units",
                                  "total_units", "items_written", "req_count",
                                  "bytes_count", "message") if c_ in df.columns]
            st.dataframe(df[cols], use_container_width=True, hide_index=True)
        else:
            st.info("No jobs yet.")
        st.caption(f"module build {BUILD} · scraper build {sk_scraper.BUILD}")
