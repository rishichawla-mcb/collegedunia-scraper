"""
Collegedunia Scraper - Streamlit UI.

A dashboard to run and monitor the scraper:
  * Phase 1: scrape all courses
  * Phase 2: scrape the colleges offering each course (the big one)
  * Proxy panel (paste list and/or provider gateway) with a test button
  * Live progress + ETA, stop/resume
  * Filter and preview the data, download as Excel / CSV / JSON
  * Run history

The actual scraping runs in a detached worker subprocess (worker.py) so it keeps
going across UI reruns and even if you close the browser tab, as long as the
host/container stays alive.

Run:  streamlit run app.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import pandas as pd
import streamlit as st

import db
import export
import scraper

HERE = os.path.dirname(os.path.abspath(__file__))

st.set_page_config(page_title="Collegedunia Scraper", page_icon="🎓", layout="wide")
db.init_db()


# ---------------------------------------------------------------------------
# Authentication (optional password gate)
# ---------------------------------------------------------------------------
def _expected_password():
    """Password comes from the APP_PASSWORD env var (or Streamlit secrets).
    If none is set, the app runs open — handy for local development."""
    pw = os.environ.get("APP_PASSWORD")
    if pw:
        return pw
    try:
        return st.secrets["APP_PASSWORD"]
    except Exception:
        return None


def require_login() -> None:
    pw = _expected_password()
    if not pw:
        return  # no password configured -> open access
    if st.session_state.get("authed"):
        return
    st.markdown("## 🔒 Collegedunia Scraper")
    st.caption("This app is password protected.")
    entered = st.text_input("Password", type="password")
    if st.button("Log in"):
        if entered == pw:
            st.session_state["authed"] = True
            st.rerun()
        else:
            st.error("Incorrect password.")
    st.stop()


require_login()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def launch_worker(job_id: int) -> None:
    """Start worker.py as a detached background process."""
    creationflags = 0
    kwargs = {}
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000008  # DETACHED_PROCESS
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen(
        [sys.executable, os.path.join(HERE, "worker.py"), "--job", str(job_id)],
        cwd=HERE, creationflags=creationflags,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kwargs,
    )


def read_log(job_id: int, lines: int = 25) -> str:
    path = os.path.join(HERE, "logs", f"job_{job_id}.log")
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as fh:
        return "".join(fh.readlines()[-lines:])


def proxy_config_from_ui() -> dict:
    return {
        "proxy_mode": st.session_state.get("proxy_mode", "none"),
        "proxy_gateway": st.session_state.get("proxy_gateway", ""),
        "proxy_list": [p.strip() for p in st.session_state.get("proxy_list_text", "").splitlines() if p.strip()],
        "proxy_cooldown": st.session_state.get("proxy_cooldown", 120),
        "delay": st.session_state.get("delay", 1.0),
        "max_retries": st.session_state.get("max_retries", 5),
        "backoff": st.session_state.get("backoff", 4.0),
    }


def fmt_eta(done: int, total: int, started: float) -> str:
    if not done or not total or not started:
        return "—"
    elapsed = time.time() - started
    rate = done / elapsed if elapsed else 0
    if rate <= 0:
        return "—"
    remaining = (total - done) / rate
    mins = remaining / 60
    if mins < 60:
        return f"~{mins:.0f} min"
    return f"~{mins/60:.1f} hr"


# ---------------------------------------------------------------------------
# Sidebar: settings + proxies
# ---------------------------------------------------------------------------
st.sidebar.title("⚙️ Settings")

st.sidebar.subheader("Rate limiting")
st.session_state["delay"] = st.sidebar.number_input(
    "Delay between requests (sec)", 0.0, 30.0,
    value=float(db.get_setting("delay", 1.0)), step=0.5,
    help="Higher = politer and less likely to be blocked.")
st.session_state["max_retries"] = st.sidebar.number_input("Max retries per request", 1, 15, 5)
st.session_state["backoff"] = st.sidebar.number_input("Backoff base (sec)", 1.0, 30.0, 4.0, step=1.0)

st.sidebar.subheader("🔀 Proxies / IP switching")
st.session_state["proxy_mode"] = st.sidebar.selectbox(
    "Mode", ["none", "list", "gateway"],
    index=["none", "list", "gateway"].index(db.get_setting("proxy_mode", "none")),
    help="none = your own IP. list = rotate a list. gateway = a provider endpoint "
         "that rotates IPs for you.")

if st.session_state["proxy_mode"] == "list":
    st.session_state["proxy_list_text"] = st.sidebar.text_area(
        "Proxy list (one per line)",
        value=db.get_setting("proxy_list_text", ""),
        height=140,
        placeholder="http://user:pass@1.2.3.4:8000\nhttp://5.6.7.8:3128",
    )
    st.session_state["proxy_cooldown"] = st.sidebar.number_input(
        "Cooldown after fail (sec)", 10, 1800, 120, step=10)
elif st.session_state["proxy_mode"] == "gateway":
    st.session_state["proxy_gateway"] = st.sidebar.text_input(
        "Gateway URL", value=db.get_setting("proxy_gateway", ""),
        placeholder="http://user:pass@gateway.provider.com:7777")

if st.sidebar.button("💾 Save settings"):
    db.set_setting("delay", st.session_state["delay"])
    db.set_setting("proxy_mode", st.session_state["proxy_mode"])
    db.set_setting("proxy_list_text", st.session_state.get("proxy_list_text", ""))
    db.set_setting("proxy_gateway", st.session_state.get("proxy_gateway", ""))
    st.sidebar.success("Saved.")

if st.sidebar.button("🧪 Test proxies"):
    urls = []
    if st.session_state["proxy_mode"] == "list":
        urls = [p.strip() for p in st.session_state.get("proxy_list_text", "").splitlines() if p.strip()]
    elif st.session_state["proxy_mode"] == "gateway" and st.session_state.get("proxy_gateway"):
        urls = [st.session_state["proxy_gateway"]]
    if not urls:
        st.sidebar.warning("No proxies to test.")
    else:
        with st.sidebar:
            for u in urls[:10]:
                res = scraper.test_proxy(u)
                if res["ok"]:
                    st.success(f"OK {res['ip']} ({res['ms']}ms)")
                else:
                    st.error(f"FAIL {res['error']}")


# ---------------------------------------------------------------------------
# Header + live counts
# ---------------------------------------------------------------------------
st.title("🎓 Collegedunia Course & College Scraper")
c = db.counts()
m1, m2, m3, m4 = st.columns(4)
m1.metric("Courses", f"{c['courses']:,}")
m2.metric("Unique colleges", f"{c['colleges']:,}")
m3.metric("Offerings (course×college)", f"{c['offerings']:,}")
m4.metric("Courses done (phase 2)", f"{c['courses_done_phase2']:,}")

tab_run, tab_data, tab_history = st.tabs(["▶️ Run", "📊 Data & export", "🕓 History"])


# ---------------------------------------------------------------------------
# Run tab
# ---------------------------------------------------------------------------
with tab_run:
    colA, colB = st.columns(2)

    # ---- Phase 1 ----
    with colA:
        st.subheader("Phase 1 — Courses")
        st.caption("Scrapes every course (~21,500). Fast on a clean IP; "
                   "datacenter IPs (incl. Streamlit Cloud) get rate-limited.")
        resume_page = int(db.get_setting("courses_resume_page", 1))
        if resume_page > 1:
            st.info(f"⏯️ A previous run stopped early. Starting will **resume from "
                    f"page {resume_page}** (≈{(resume_page-1)*10} courses already saved). "
                    f"Already-scraped courses are skipped automatically.")
        test1 = st.checkbox("Test run (first 3 pages only)", key="t1")
        restart1 = st.checkbox("Force restart from page 1", key="r1",
                               help="Ignore the saved resume point and re-scrape from the top "
                                    "(existing rows are updated, not duplicated).")
        if st.button("▶️ Start course scrape", type="primary", key="run1"):
            cfg = proxy_config_from_ui()
            if test1:
                cfg["max_pages"] = 3
            cfg["force_restart"] = restart1
            jid = db.create_job("courses", cfg)
            launch_worker(jid)
            st.session_state["watch_job"] = jid
            st.success(f"Started job #{jid}")

    # ---- Phase 2 ----
    with colB:
        st.subheader("Phase 2 — Colleges per course")
        st.caption("For each course, scrapes all colleges offering it. "
                   "Huge: can be hundreds of thousands of requests. Scope it!")
        if c["courses"] == 0:
            st.info("Run Phase 1 first so there are courses to expand.")
        scope = st.radio("Scope", ["Top N courses by college count", "Filter by stream/type", "All courses"],
                         key="scope2")
        cfg2_extra = {}
        if scope == "Top N courses by college count":
            n = st.number_input("N", 1, 21500, 50, key="topn")
            cfg2_extra["course_ids"] = db.list_course_ids()[: int(n)]
        elif scope == "Filter by stream/type":
            with db.connect() as conn:
                streams = [r[0] for r in conn.execute(
                    "SELECT DISTINCT stream_id FROM courses WHERE stream_id<>'' ORDER BY stream_id")]
                ctypes = [r[0] for r in conn.execute(
                    "SELECT DISTINCT course_type FROM courses WHERE course_type<>'' ORDER BY course_type")]
            sel_stream = st.multiselect("Stream IDs", streams)
            sel_type = st.multiselect("Course types", ctypes)
            wheres, params = [], []
            if sel_stream:
                wheres.append(f"stream_id IN ({','.join('?'*len(sel_stream))})")
                params += sel_stream
            if sel_type:
                wheres.append(f"course_type IN ({','.join('?'*len(sel_type))})")
                params += sel_type
            cfg2_extra["course_where"] = " AND ".join(wheres)
            cfg2_extra["course_where_params"] = params
        test2 = st.checkbox("Test run (max 2 pages per course)", key="t2")
        force = st.checkbox("Force re-scrape (ignore resume)", key="f2")
        if st.button("▶️ Start college scrape", type="primary", key="run2",
                     disabled=c["courses"] == 0):
            cfg = proxy_config_from_ui()
            cfg.update(cfg2_extra)
            if test2:
                cfg["max_pages_per_course"] = 2
            cfg["force_rescrape"] = force
            jid = db.create_job("offerings", cfg)
            launch_worker(jid)
            st.session_state["watch_job"] = jid
            st.success(f"Started job #{jid}")

    st.divider()

    # ---- Live monitor ----
    st.subheader("Live progress")
    running = [j for j in db.list_jobs(10) if j["status"] in ("queued", "running")]
    watch_id = st.session_state.get("watch_job")
    job = None
    if watch_id:
        job = db.get_job(watch_id)
    elif running:
        job = running[0]

    if job:
        total = job["total_units"] or 0
        done = job["done_units"] or 0
        pct = (done / total) if total else 0.0
        st.progress(min(pct, 1.0), text=f"Job #{job['id']} ({job['type']}) — {job['status']}")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Processed", f"{done:,}" + (f" / {total:,}" if total else ""))
        k2.metric("Items written", f"{job['items_written']:,}")
        k3.metric("ETA", fmt_eta(done, total, job["started_at"]))
        k4.metric("Status", job["status"])
        st.caption(job.get("message") or "")
        if job["status"] in ("running", "queued"):
            if st.button("⏹️ Stop this job"):
                db.request_stop(job["id"])
                st.warning("Stop requested — the worker will finish its current page and exit.")
        st.code(read_log(job["id"]) or "(waiting for log…)", language="text")
        if job["status"] in ("running", "queued"):
            time.sleep(3)
            st.rerun()
    else:
        st.info("No active job. Start one above.")


# ---------------------------------------------------------------------------
# Data tab
# ---------------------------------------------------------------------------
with tab_data:
    table = st.selectbox("Table", ["courses", "colleges", "offerings"])
    with db.connect() as conn:
        ncols = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    st.caption(f"{ncols:,} rows in `{table}`")

    q = st.text_input("Quick filter (matches name / college / city — leave blank for all)")
    limit = st.slider("Preview rows", 10, 2000, 200)
    where, params = "", []
    if q:
        if table == "courses":
            where = "WHERE name LIKE ?"
            params = [f"%{q}%"]
        elif table == "colleges":
            where = "WHERE name LIKE ? OR city LIKE ?"
            params = [f"%{q}%", f"%{q}%"]
        else:
            where = "WHERE course_name LIKE ? OR college_name LIKE ? OR city LIKE ?"
            params = [f"%{q}%", f"%{q}%", f"%{q}%"]
    with db.connect() as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table} {where} LIMIT {int(limit)}", conn, params=params)
    st.dataframe(df, use_container_width=True, height=420)

    st.subheader("Download")
    d1, d2, d3, d4 = st.columns(4)
    with d1:
        st.download_button("⬇️ All tables (.xlsx)", data=export.to_xlsx(),
                           file_name="collegedunia_all.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with d2:
        st.download_button(f"⬇️ {table} (.csv)", data=export.to_csv(table),
                           file_name=f"collegedunia_{table}.csv", mime="text/csv")
    with d3:
        st.download_button(f"⬇️ {table} (.json)", data=export.to_json(table),
                           file_name=f"collegedunia_{table}.json", mime="application/json")
    with d4:
        st.download_button(f"⬇️ {table} (.xlsx)", data=export.to_xlsx((table,)),
                           file_name=f"collegedunia_{table}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ---------------------------------------------------------------------------
# History tab
# ---------------------------------------------------------------------------
with tab_history:
    jobs = db.list_jobs(50)
    if not jobs:
        st.info("No runs yet.")
    else:
        rows = []
        for j in jobs:
            rows.append({
                "id": j["id"], "type": j["type"], "status": j["status"],
                "processed": j["done_units"], "of": j["total_units"],
                "items": j["items_written"],
                "started": time.strftime("%Y-%m-%d %H:%M", time.localtime(j["started_at"])) if j["started_at"] else "",
                "message": (j.get("message") or "")[:80],
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=460)
        if st.button("🔄 Refresh"):
            st.rerun()
