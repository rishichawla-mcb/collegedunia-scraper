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

import hashlib
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


def _auth_token():
    pw = _expected_password()
    return hashlib.sha256(f"cd::{pw}".encode()).hexdigest()[:24] if pw else None


def require_login() -> None:
    pw = _expected_password()
    if not pw:
        return  # no password configured -> open access
    tok = _auth_token()
    # Auto-login from a URL token so login survives idle reconnects / restarts.
    try:
        if st.query_params.get("t") == tok:
            st.session_state["authed"] = True
    except Exception:
        pass
    if st.session_state.get("authed"):
        return
    st.markdown("## 🔒 Collegedunia Scraper")
    st.caption("This app is password protected.")
    entered = st.text_input("Password", type="password")
    if st.button("Log in"):
        if entered == pw:
            st.session_state["authed"] = True
            try:
                st.query_params["t"] = tok   # keeps you logged in across reloads
            except Exception:
                pass
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
        "adaptive": st.session_state.get("adaptive", True),
        "long_cooldown_seconds": st.session_state.get("long_cooldown", 0),
        "webhook_url": db.get_setting("webhook_url", "") or None,
        "smtp": db.get_setting("smtp", {}) or {},
    }


def human_mb(byts) -> str:
    try:
        return f"{(byts or 0)/1048576:.1f} MB"
    except Exception:
        return "0 MB"


# --- Cached SQL aggregations for the dashboards (tiny result sets, computed at
# --- most every TTL seconds — never loads full tables into memory). ---
@st.cache_data(ttl=15, show_spinner=False)
def agg_group(col: str) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            f"SELECT {col} AS k, COUNT(*) AS n FROM courses WHERE {col}<>'' "
            f"GROUP BY {col} ORDER BY n DESC", conn)


@st.cache_data(ttl=20, show_spinner=False)
def agg_city_counts(n: int) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT city AS k, COUNT(DISTINCT college_id) AS n FROM offerings "
            "WHERE city<>'' GROUP BY city ORDER BY n DESC LIMIT ?", conn, params=[int(n)])


@st.cache_data(ttl=20, show_spinner=False)
def agg_fee_buckets() -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT CASE "
            "WHEN fees_amount<=25000 THEN '0-25k' WHEN fees_amount<=50000 THEN '25-50k' "
            "WHEN fees_amount<=75000 THEN '50-75k' WHEN fees_amount<=100000 THEN '75k-1L' "
            "WHEN fees_amount<=200000 THEN '1-2L' WHEN fees_amount<=500000 THEN '2-5L' "
            "ELSE '5L+' END AS k, COUNT(*) AS n, MIN(fees_amount) AS mn "
            "FROM offerings WHERE fees_amount>0 GROUP BY k ORDER BY mn", conn)


@st.cache_data(ttl=20, show_spinner=False)
def agg_top_ranked() -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT college_name, course_name, ranking_rank, ranking_agency, city, "
            "fees_amount, course_rating FROM offerings WHERE ranking_rank>0 "
            "ORDER BY ranking_rank LIMIT 50", conn)


@st.cache_data(ttl=20, show_spinner=False)
def agg_coverage() -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT c.stream_name AS stream, COUNT(*) AS courses, "
            "SUM(CASE WHEN op.status='done' THEN 1 ELSE 0 END) AS done "
            "FROM courses c LEFT JOIN offering_progress op ON c.course_id=op.course_id "
            "WHERE c.stream_name IS NOT NULL AND c.stream_name<>'' "
            "GROUP BY c.stream_name ORDER BY courses DESC", conn)


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
st.session_state["adaptive"] = st.sidebar.checkbox(
    "Adaptive throttle", value=True,
    help="Auto-slow when blocks happen, speed back up when clean.")
st.session_state["long_cooldown"] = st.sidebar.number_input(
    "Long cooldown on block (sec, 0=off)", 0, 1800, 0, step=30,
    help="On a soft block, wait this long before retrying (rides out rate-limit windows). 0 = short backoff.")

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

with st.sidebar.expander("🔔 Notifications"):
    wh = st.text_input("Webhook URL (Slack/Discord/generic)",
                       value=db.get_setting("webhook_url", ""),
                       placeholder="https://hooks.slack.com/services/...")
    st.caption("Email (optional)")
    smtp = db.get_setting("smtp", {}) or {}
    s_host = st.text_input("SMTP host", value=smtp.get("host", ""))
    c1, c2 = st.columns(2)
    s_port = c1.text_input("Port", value=str(smtp.get("port", 587)))
    s_to = c2.text_input("Send to", value=smtp.get("to", ""))
    s_user = st.text_input("SMTP user", value=smtp.get("user", ""))
    s_pass = st.text_input("SMTP password", type="password", value=smtp.get("password", ""))
    if st.button("Save notifications"):
        db.set_setting("webhook_url", wh.strip())
        db.set_setting("smtp", {"host": s_host.strip(), "port": s_port.strip(),
                                "to": s_to.strip(), "user": s_user.strip(),
                                "password": s_pass, "from": s_user.strip()})
        st.success("Notification settings saved.")

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

tab_run, tab_report, tab_index, tab_data, tab_history = st.tabs(
    ["▶️ Run", "📈 Reporting", "🗂️ Indexing", "📊 Data & export", "🕓 History"])


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

        st.markdown("---")
        st.markdown("**Complete scrape (recommended)**")
        st.caption("The plain scrape above hits the site's ~1,700-result page limit "
                   "(~1,200 courses). This splits the catalog by stream/type/level to "
                   "pull **all ~21,500**. Needs the proxy on; takes longer.")
        if st.button("🧩 Scrape ALL courses (partitioned)", key="run1full"):
            cfg = proxy_config_from_ui()
            cfg["partition"] = True
            jid = db.create_job("courses", cfg)
            launch_worker(jid)
            st.session_state["watch_job"] = jid
            st.success(f"Started complete scrape — job #{jid}")

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
                ctypes = [r[0] for r in conn.execute(
                    "SELECT DISTINCT course_type FROM courses WHERE course_type<>'' ORDER BY course_type")]
            stream_ids = [str(s) for s in sorted(db.STREAMS, key=lambda s: db.STREAMS[s])]
            sel_stream = st.multiselect(
                "Streams", stream_ids,
                format_func=lambda s: f"{db.stream_name(s)} ({s})")
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
        order = st.selectbox("Process order",
                             ["colleges_desc", "colleges_asc", "stream"],
                             format_func={"colleges_desc": "Biggest courses first",
                                          "colleges_asc": "Smallest first (bank quick wins)",
                                          "stream": "Grouped by stream"}.get,
                             key="order2")
        cfg2_extra["order"] = order

        cc1, cc2 = st.columns(2)
        concurrency = cc1.number_input("Parallel workers", 1, 20, 1, key="conc2",
                                       help="Concurrent requests. Use >1 only with a working proxy.")
        skip_empty = cc2.checkbox("Skip 0-college courses", value=True, key="skip2")

        st.markdown("**Budget caps** (0 = unlimited)")
        b1, b2 = st.columns(2)
        budget_mb = b1.number_input("Max bandwidth (MB)", 0, 100000, 0, step=50, key="bmb",
                                    help="Stop when this much has been downloaded. Protects proxy spend.")
        budget_req = b2.number_input("Max requests", 0, 5_000_000, 0, step=1000, key="breq")

        with st.expander("Advanced: extra API filters (city/state etc.)"):
            scope_raw = st.text_area(
                "Extra filters (JSON merged into each request)",
                placeholder='{"city": 4337}   # optional, advanced',
                key="scopejson")

        test2 = st.checkbox("Test run (max 2 pages per course)", key="t2")
        force = st.checkbox("Force re-scrape (ignore resume)", key="f2")
        if st.button("▶️ Start college scrape", type="primary", key="run2",
                     disabled=c["courses"] == 0):
            cfg = proxy_config_from_ui()
            cfg.update(cfg2_extra)
            cfg["concurrency"] = int(concurrency)
            cfg["skip_empty"] = bool(skip_empty)
            cfg["budget_mb"] = float(budget_mb)
            cfg["budget_requests"] = int(budget_req)
            if scope_raw.strip():
                try:
                    cfg["scope_filters"] = json.loads(scope_raw)
                except json.JSONDecodeError:
                    st.error("Extra filters must be valid JSON — ignoring."); cfg["scope_filters"] = {}
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
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Processed", f"{done:,}" + (f" / {total:,}" if total else ""))
        k2.metric("Items written", f"{job['items_written']:,}")
        k3.metric("Bandwidth", human_mb(job.get("bytes_count")))
        k4.metric("ETA", fmt_eta(done, total, job["started_at"]))
        k5.metric("Status", job["status"])
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
# Reporting dashboard (interactive)
# ---------------------------------------------------------------------------
with tab_report:
    st.subheader("📈 Reporting")
    rc = db.counts()
    if rc["courses"] == 0:
        st.info("No data yet — run a scrape first.")
    else:
        if st.button("🛠️ Prepare analytics export"):
            with st.spinner("Building…"):
                st.session_state["andata"] = export.to_analytics_xlsx()
        if st.session_state.get("andata"):
            st.download_button("⬇️ Analytics summary (.xlsx)", data=st.session_state["andata"],
                               file_name="collegedunia_analytics.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        bs = agg_group("stream_name").rename(columns={"k": "stream"}).set_index("stream")["n"]
        bt = agg_group("course_type").rename(columns={"k": "type"}).set_index("type")["n"]
        bl = agg_group("level").rename(columns={"k": "level"}).set_index("level")["n"]
        a, b = st.columns(2)
        with a:
            st.markdown("**Courses by stream**"); st.bar_chart(bs)
        with b:
            st.markdown("**Courses by type**"); st.bar_chart(bt)
        st.markdown("**Courses by level**"); st.bar_chart(bl)

        if rc["offerings"] > 0:
            st.divider()
            st.markdown("### College / fee / ranking analytics (Phase 2)")
            topn = st.slider("Top N cities", 5, 50, 15)
            cc = agg_city_counts(topn).set_index("k")["n"]
            st.markdown("**Unique colleges by city**"); st.bar_chart(cc)
            fb = agg_fee_buckets()
            if not fb.empty:
                st.markdown("**1st-year fee distribution (₹)**")
                st.bar_chart(fb.set_index("k")["n"])
            tr = agg_top_ranked()
            if not tr.empty:
                st.markdown("**Top-ranked offerings**")
                st.dataframe(tr, use_container_width=True, height=320)
        else:
            st.caption("Run Phase 2 to unlock college / fee / ranking analytics.")


# ---------------------------------------------------------------------------
# Indexing & coverage dashboard (interactive)
# ---------------------------------------------------------------------------
with tab_index:
    st.subheader("🗂️ Indexing & coverage")
    ic = db.counts()
    with db.connect() as conn:
        done = conn.execute("SELECT COUNT(*) FROM offering_progress WHERE status='done'").fetchone()[0]
        partial = conn.execute("SELECT COUNT(*) FROM offering_progress WHERE status='partial'").fetchone()[0]
    total_courses = ic["courses"]
    pending = max(0, total_courses - done - partial)
    i1, i2, i3, i4 = st.columns(4)
    i1.metric("Courses indexed", f"{total_courses:,}")
    i2.metric("Phase-2 done", f"{done:,}")
    i3.metric("Partial", f"{partial:,}")
    i4.metric("Pending", f"{pending:,}")
    if total_courses:
        st.progress(done / total_courses, text=f"Phase-2 coverage: {done:,}/{total_courses:,} courses")

    if total_courses:
        cov = agg_coverage()
        if not cov.empty:
            cov["done"] = cov["done"].fillna(0).astype(int)
            cov["% done"] = (cov["done"] / cov["courses"] * 100).round(1)
            st.markdown("**Phase-2 coverage by stream**")
            st.dataframe(cov, use_container_width=True, height=300)

    st.divider()
    st.markdown("### 🔎 Browse index")
    which = st.radio("Index", ["courses", "colleges"], horizontal=True)
    term = st.text_input("Search by name", key="idxsearch")
    with db.connect() as conn:
        if which == "courses":
            sql = ("SELECT course_id, name, stream_name, course_type, level, colleges_count "
                   "FROM courses")
            params = []
            if term:
                sql += " WHERE name LIKE ?"; params = [f"%{term}%"]
            sql += " ORDER BY colleges_count DESC LIMIT 500"
        else:
            sql = "SELECT college_id, name, short_form, city, state_id FROM colleges"
            params = []
            if term:
                sql += " WHERE name LIKE ? OR city LIKE ?"; params = [f"%{term}%", f"%{term}%"]
            sql += " ORDER BY name LIMIT 500"
        idf = pd.read_sql_query(sql, conn, params=params)
    st.caption(f"{len(idf):,} shown (max 500)")
    st.dataframe(idf, use_container_width=True, height=340)

    if total_courses:
        st.divider()
        st.markdown("**Pending courses (not yet Phase-2 done)** — biggest first")
        with db.connect() as conn:
            pend = pd.read_sql_query(
                "SELECT c.course_id, c.name, c.stream_name, c.colleges_count "
                "FROM courses c LEFT JOIN offering_progress op ON c.course_id=op.course_id "
                "WHERE op.course_id IS NULL OR op.status<>'done' "
                "ORDER BY c.colleges_count DESC LIMIT 200", conn)
        st.dataframe(pend, use_container_width=True, height=300)


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
    st.caption("Exports are built only when you click Prepare (keeps memory low).")
    fmt = st.radio("Format", ["CSV", "JSON", "Excel (this table)", "Excel (all tables)"],
                   horizontal=True, key="expfmt")
    if st.button("🛠️ Prepare download"):
        with st.spinner("Building export…"):
            if fmt == "CSV":
                st.session_state["expdata"] = (export.to_csv(table),
                                               f"collegedunia_{table}.csv", "text/csv")
            elif fmt == "JSON":
                st.session_state["expdata"] = (export.to_json(table),
                                               f"collegedunia_{table}.json", "application/json")
            elif fmt == "Excel (this table)":
                st.session_state["expdata"] = (
                    export.to_xlsx((table,)), f"collegedunia_{table}.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.session_state["expdata"] = (
                    export.to_xlsx(), "collegedunia_all.xlsx",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    if st.session_state.get("expdata"):
        data, fname, mime = st.session_state["expdata"]
        st.download_button(f"⬇️ Download {fname}", data=data, file_name=fname, mime=mime)


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
