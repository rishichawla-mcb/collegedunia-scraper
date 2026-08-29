"""
Course Finder — UI section rendered inside app.py behind the vertical switch.

Reads and writes ONLY `cf_` tables. Uses the SAME proxy the rest of the app has
configured. Scrapes run as detached worker processes (platform_worker.py).
"""
from __future__ import annotations

BUILD = "2026-08-29a"

import os
import subprocess
import sys
import time

import pandas as pd
import streamlit as st

import cf_db
import cf_vertical  # noqa: F401  registers 'coursefinder'
_VERTICALS = (cf_vertical,)   # referenced so pyflakes keeps the registration import
import db as _core

SECS_PER_REQ = 19.0     # observed round-trip through the proxy


def _launch(job_id: int) -> None:
    try:
        subprocess.Popen([sys.executable, "platform_worker.py", "coursefinder",
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
    return "direct (no proxy)"


def _eta(requests: int, concurrency: int) -> str:
    if not requests:
        return "—"
    secs = requests * SECS_PER_REQ / max(1, concurrency)
    if secs < 3600:
        return f"~{secs/60:.0f} min"
    if secs < 86400:
        return f"~{secs/3600:.1f} h"
    return f"~{secs/86400:.1f} days"


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
    jobs = cf_db.list_jobs(limit=10)
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
    k[4].metric("Bandwidth", f"{(j.get('bytes_count') or 0)/1048576:,.0f} MB")
    st.caption(j.get("message") or "")
    if tot:
        st.progress(min(1.0, done / tot))
    c1, c2 = st.columns(2)
    if j["status"] == "running" and c1.button("⏹️ Stop", key=f"stop{key}{j['id']}"):
        cf_db.request_stop(j["id"])
        st.warning("Stop requested — the worker finishes its current item and exits.")
    if c2.button("🔄 Refresh", key=f"ref{key}{j['id']}"):
        st.rerun()
    with st.expander("Log", expanded=False):
        st.code("\n".join(r["message"] for r in cf_db.get_logs(j["id"], 200)) or "—")


def render() -> None:
    cf_db.init_db()
    st.title("🔎 Course Finder")
    st.caption(f"collegedunia.com/course-finder · proxy: {_proxy_line()} · "
               f"self-contained (`cf_` tables only — no other module reads or "
               f"writes them)")

    c = cf_db.counts()
    fc = cf_db.phase_b_forecast()

    m = st.columns(5)
    m[0].metric("Courses", f"{c['courses']:,}")
    m[1].metric("Offerings", f"{c['offerings']:,}")
    m[2].metric("Distinct colleges", f"{c['distinct_colleges']:,}")
    m[3].metric("Courses scraped", f"{c['courses_scraped']:,}",
                f"{c['courses'] - c['courses_scraped']:,} left" if c["courses"] else None)
    m[4].metric("Partitions done", f"{c['partitions_done']:,}")

    tab_a, tab_b, tab_data, tab_hist = st.tabs(
        ["Ⓐ Catalogue", "Ⓑ Offerings", "Data", "History"])

    # ----------------------------------------------------------------- Ⓐ
    with tab_a:
        st.subheader("Ⓐ Catalogue — the full course list")
        st.caption("Sweeps the course-finder listing sliced by `course_tag_id` "
                   "(the unsliced listing caps at ~1,700 results). Also captures "
                   "**how many colleges offer each course** — free in the listing — "
                   "which is what makes Phase Ⓑ costable before you run it.")
        a1, a2, a3 = st.columns(3)
        conc_a = a1.number_input("Parallel workers", 1, 20, 4, key="cfa_conc")
        budget_a = a2.number_input("Bandwidth budget (MB, 0 = none)", 0, 20000, 0,
                                   step=100, key="cfa_mb")
        force_a = a3.checkbox("Restart from scratch", value=False, key="cfa_force",
                              help="Off = resume, skipping partitions already done.")
        st.caption(f"~21,700 courses ≈ 2,200–3,500 requests → "
                   f"**{_eta(3000, int(conc_a))}** at {int(conc_a)} workers.")
        if st.button("▶️ Run catalogue sweep", type="primary", key="cfa_run"):
            jid = cf_db.create_job("catalogue", _cfg(
                concurrency=int(conc_a), budget_mb=float(budget_a),
                force_restart=bool(force_a)))
            _launch(jid)
            st.success(f"Started catalogue sweep — job #{jid}")
            time.sleep(1)
            st.rerun()
        _job_monitor("a")

    # ----------------------------------------------------------------- Ⓑ
    with tab_b:
        st.subheader("Ⓑ Offerings — colleges offering each course")
        if not c["courses"]:
            st.info("Run the catalogue sweep first — Phase Ⓑ reads its queue from it.")
        else:
            st.markdown("**Forecast** (from `colleges_count`, no requests spent)")
            f = st.columns(4)
            f[0].metric("Courses pending", f"{fc['courses_left']:,}")
            f[1].metric("Offerings expected", f"{fc['offerings_left']:,}")
            f[2].metric("Requests needed", f"{fc['pages_left']:,}")
            f[3].metric("Est. disk", f"{fc['offerings_left']*2.4/1000:,.1f} GB")

            b1, b2, b3 = st.columns(3)
            conc_b = b1.number_input("Parallel workers", 1, 20, 8, key="cfb_conc")
            maxc = b2.number_input("Max courses this run (0 = all)", 0, 25000, 0,
                                   step=100, key="cfb_max")
            minc = b3.number_input("Skip courses with fewer colleges than", 0, 500, 0,
                                   step=5, key="cfb_min")
            b4, b5 = st.columns(2)
            order = b4.selectbox("Order", ["value", "id"], key="cfb_order",
                                 help="'value' = most colleges first, so a "
                                      "budget-limited run captures the most data.")
            keep_raw = b5.checkbox("Store raw payload per offering", value=True,
                                   key="cfb_raw",
                                   help="Roughly doubles the disk cost. Turn OFF "
                                        "for the full run if space is tight — "
                                        "every parsed column is kept either way.")
            budget_b = st.number_input("Bandwidth budget (MB, 0 = none)", 0, 50000, 0,
                                       step=500, key="cfb_mb")
            st.caption(f"**{_eta(fc['pages_left'], int(conc_b))}** at "
                       f"{int(conc_b)} workers.")
            if st.button("▶️ Run offerings crawl", type="primary", key="cfb_run"):
                jid = cf_db.create_job("offerings", _cfg(
                    concurrency=int(conc_b), max_courses=int(maxc),
                    min_colleges=int(minc), order=order,
                    keep_raw=bool(keep_raw), budget_mb=float(budget_b)))
                _launch(jid)
                st.success(f"Started offerings crawl — job #{jid}")
                time.sleep(1)
                st.rerun()
        _job_monitor("b")

    # --------------------------------------------------------------- Data
    with tab_data:
        st.subheader("Data")
        which = st.selectbox("Table", ["cf_courses", "cf_offerings",
                                       "cf_course_progress",
                                       "cf_partition_progress"], key="cfd_tbl")
        with cf_db.connect() as conn:
            n = conn.execute(f"SELECT COUNT(*) FROM {which}").fetchone()[0]
            st.caption(f"{n:,} rows")
            if n:
                df = pd.read_sql_query(f"SELECT * FROM {which} LIMIT 300", conn)
                drop = [c_ for c_ in ("raw_json",) if c_ in df.columns]
                st.dataframe(df.drop(columns=drop), use_container_width=True,
                             height=420)
        if which == "cf_courses":
            with cf_db.connect() as conn:
                top = pd.read_sql_query(
                    "SELECT name, colleges_count FROM cf_courses "
                    "WHERE COALESCE(colleges_count,0)>0 "
                    "ORDER BY colleges_count DESC LIMIT 20", conn)
            if not top.empty:
                st.markdown("**Biggest courses by college count**")
                st.dataframe(top, use_container_width=True, hide_index=True)

    # ------------------------------------------------------------ History
    with tab_hist:
        st.subheader("History")
        jobs = cf_db.list_jobs(limit=50)
        if jobs:
            df = pd.DataFrame(jobs)
            cols = [c_ for c_ in ("id", "phase", "status", "done_units",
                                  "total_units", "items_written", "req_count",
                                  "bytes_count", "message") if c_ in df.columns]
            st.dataframe(df[cols], use_container_width=True, hide_index=True)
        else:
            st.info("No jobs yet.")
