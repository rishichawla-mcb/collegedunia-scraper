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


def _pid_alive(pid) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


# Recover jobs whose worker died (container restart / crash) — once per session.
if not st.session_state.get("_recovered"):
    st.session_state["_recovered"] = True
    for _j in db.list_jobs(20):
        if _j["status"] in ("running", "queued") and not _pid_alive(_j.get("pid")):
            # The worker died (crash / OOM-kill / restart). Save its staged data:
            # in incremental mode the bulk is already in master; flush the tail too
            # so nothing scraped is lost. (Strict-gate jobs keep their staging for
            # manual review.)
            try:
                _jcfg = json.loads(_j.get("config_json") or "{}")
            except Exception:
                _jcfg = {}
            _saved = ""
            try:
                if _jcfg.get("incremental_promote", True) and db.staged_summary(_j["id"]):
                    db.flush_job_staging(_j["id"])
                    db.update_job(_j["id"], promote_status="promoted")
                    _saved = " — staged data promoted to master"
            except Exception:
                pass
            db.update_job(_j["id"], status="stopped",
                          message=f"interrupted (worker not running){_saved}; resume to continue")


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


@st.cache_resource
def _start_scheduler():
    """Singleton daemon (one per server) that fires scheduled refreshes when due."""
    import threading as _th
    import time as _t

    def loop():
        while True:
            try:
                s = db.get_setting("schedule", {}) or {}
                if s.get("enabled") and _t.time() >= float(s.get("next_run") or 0):
                    base = {
                        "proxy_mode": db.get_setting("proxy_mode", "none"),
                        "proxy_gateway": db.get_setting("proxy_gateway", ""),
                        "proxy_list": [p.strip() for p in
                                       db.get_setting("proxy_list_text", "").splitlines() if p.strip()],
                        "delay": db.get_setting("delay", 1.0),
                    }
                    jt = s.get("job_type", "courses")
                    if jt == "courses":
                        base["partition"] = True
                    jid = db.create_job(jt, base)
                    launch_worker(jid)
                    db.add_snapshot(f"scheduled {jt}")
                    s["next_run"] = _t.time() + int(s.get("interval_sec", 604800))
                    db.set_setting("schedule", s)
            except Exception:
                pass
            _t.sleep(60)

    th = _th.Thread(target=loop, daemon=True)
    th.start()
    return th


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
        # governance: stage -> validate -> promote
        "staging": st.session_state.get("staging", True),
        "auto_promote": st.session_state.get("auto_promote", True),
        "incremental_promote": st.session_state.get("incremental_promote", True),
        "validation_rules": {
            "min_rows": int(st.session_state.get("v_min_rows", 1)),
            "max_missing_fee_pct": float(st.session_state.get("v_max_missing", 100)),
            "pass_score": float(st.session_state.get("v_pass_score", 70)),
        },
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


@st.cache_data(ttl=30, show_spinner=False)
def agg_colleges_by_city(n: int) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT city AS k, COUNT(*) AS n FROM colleges WHERE city<>'' "
            "GROUP BY city ORDER BY n DESC LIMIT ?", conn, params=[int(n)])


@st.cache_data(ttl=30, show_spinner=False)
def agg_colleges_by_state() -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT CAST(state_id AS TEXT) AS k, COUNT(*) AS n FROM colleges "
            "WHERE state_id IS NOT NULL GROUP BY state_id ORDER BY n DESC LIMIT 40", conn)


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

st.sidebar.subheader("🛡️ Data governance")
st.session_state["staging"] = st.sidebar.checkbox(
    "Stage → validate → promote", value=True,
    help="Jobs write to a per-job staging area; data reaches master only after a "
         "quality check. Uncheck to write straight to master (legacy).")
st.session_state["auto_promote"] = st.sidebar.checkbox(
    "Auto-promote when QC passes", value=True,
    help="Clean jobs merge to master automatically; failed ones wait for your approval.")
st.session_state["incremental_promote"] = st.sidebar.checkbox(
    "🧠 Incremental promote (memory-safe)", value=True,
    help="Move staged rows to master continuously during the run (in small chunks) "
         "instead of only at the end. Keeps memory low and means an interrupted / "
         "OOM-killed job keeps everything scraped so far — you lose at most the last "
         "chunk, which a resume re-scrapes. Turn OFF for the strict validate-then-"
         "promote-once gate.")
st.session_state["v_pass_score"] = st.sidebar.number_input("QC pass score (0-100)", 0, 100, 70)
st.session_state["v_min_rows"] = st.sidebar.number_input("QC: min rows", 0, 100000, 1)
st.session_state["v_max_missing"] = st.sidebar.number_input("QC: max missing-fee %", 0, 100, 100)

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
_start_scheduler()  # start the scheduled-refresh daemon once per server

st.title("🎓 Collegedunia Course & College Scraper")
c = db.counts()
m1, m2, m3, m4 = st.columns(4)
m1.metric("Courses", f"{c['courses']:,}")
m2.metric("Unique colleges", f"{c['colleges']:,}")
m3.metric("Offerings (course×college)", f"{c['offerings']:,}")
m4.metric("Courses done (phase 2)", f"{c['courses_done_phase2']:,}")


def render_job_monitor(job_types, key: str, govern: bool = True) -> None:
    """Per-phase live monitor: progress, staged data (live), logs, controls —
    scoped to this phase's job (its watch id, else the latest job of job_types)."""
    watch_id = st.session_state.get(f"watch_{key}")
    job = db.get_job(watch_id) if watch_id else None
    if not job:
        for j in db.list_jobs(30):
            if j["type"] in job_types:
                job = j
                break
    st.divider()
    st.markdown("##### 📡 Live progress & logs")
    if not job:
        st.info("No run yet for this phase. Start one above.")
        return
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

    if govern and job["type"] != "enrichment":
        gj1, gj2 = st.columns(2)
        qscore = job.get("quality_score")
        gj1.metric("Quality score", f"{qscore:.0f}/100" if qscore is not None else "—")
        gj2.metric("Promotion", job.get("promote_status")
                   or ("staging…" if job["status"] in ("running", "queued") else "—"))
        staged = db.staged_summary(job["id"])
        if staged:
            jid_ = job["id"]
            st.markdown(f"###### 🔬 Staged data (live) — {sum(staged.values()):,} rows")
            with st.container(border=True):
                st.write({k: f"{v:,}" for k, v in staged.items()})
                stbl = st.selectbox("Table", list(staged.keys()), key=f"stbl{key}{jid_}")
                srows = db.get_staged_rows(jid_, stbl, limit=500)
                if srows:
                    st.dataframe(pd.DataFrame(srows), use_container_width=True, height=300)
                    st.download_button(
                        f"⬇️ Download staged {stbl} (CSV)",
                        data=pd.DataFrame(db.get_staged_rows(jid_, stbl, limit=100000))
                             .to_csv(index=False).encode("utf-8"),
                        file_name=f"job{jid_}_{stbl}_staged.csv", mime="text/csv",
                        key=f"dl{key}{jid_}{stbl}")
                if st.button("🔍 Diff vs master", key=f"diffb{key}{jid_}"):
                    st.session_state[f"diff{jid_}"] = db.diff_job(jid_)
                if st.session_state.get(f"diff{jid_}"):
                    st.json(st.session_state[f"diff{jid_}"])
                # Promote / reject is available whenever a finished job has staged
                # data that isn't promoted yet — including a job you stopped midway
                # (it won't auto-promote, but you can write its partial data here).
                finished = job["status"] not in ("running", "queued")
                if finished and job.get("promote_status") != "promoted":
                    if job.get("promote_status") not in ("pending", "rejected"):
                        st.caption("⏸️ This job didn't finish, so it wasn't auto-promoted. "
                                   "You can still write its partial staged data to master:")
                    ap1, ap2 = st.columns(2)
                    if ap1.button(f"✅ Promote staged → master ({sum(staged.values()):,} rows)",
                                  key=f"appr{key}{jid_}"):
                        summ = db.promote_job(jid_)
                        st.success(f"Promoted {sum(summ.values()):,} rows to master.")
                        st.rerun()
                    if ap2.button("🗑️ Reject (discard staged)", key=f"rej{key}{jid_}"):
                        db.discard_staging(jid_)
                        db.update_job(jid_, promote_status="rejected")
                        st.warning("Staged data discarded.")
                        st.rerun()

    if job["type"] == "enrichment":
        st.markdown("###### 🔬 Recently enriched (live)")
        with st.container(border=True), db.connect() as conn:
            edf = pd.read_sql_query(
                "SELECT college_id, name, city, website, email, phone, rating_value "
                "FROM colleges WHERE enriched_at IS NOT NULL "
                "ORDER BY enriched_at DESC LIMIT 200", conn)
        st.dataframe(edf, use_container_width=True, height=280)

    if job["status"] in ("running", "queued"):
        if st.button("⏹️ Stop this job", key=f"stop{key}"):
            db.request_stop(job["id"])
            st.warning("Stop requested — the worker will finish its current page and exit.")
    if job["status"] in ("stopped", "error"):
        if st.button("▶️ Resume this job", key=f"resume{key}"):
            db.resume_job(job["id"])
            launch_worker(job["id"])
            st.session_state[f"watch_{key}"] = job["id"]
            st.success("Resumed — continues from saved progress.")
            st.rerun()
    nlines = st.slider("Live log lines", 30, 500, 150, key=f"loglines{key}")
    logs = db.get_logs(job["id"], limit=int(nlines))
    log_text = "\n".join(l["message"] for l in reversed(logs)) or "(waiting for logs…)"
    st.code(log_text, language="text")
    lc1, lc2 = st.columns(2)
    if lc1.button("🗑️ Clear this job's logs", key=f"clr{key}"):
        db.clear_logs(job["id"])
        st.rerun()
    lc2.caption(f"{len(logs)} lines shown · auto-refreshes while running")
    if job["status"] in ("running", "queued"):
        time.sleep(3)
        st.rerun()


(tab_run, tab_query, tab_live, tab_report, tab_index, tab_data,
 tab_quality, tab_history) = st.tabs(
    ["▶️ Run", "🔎 Query", "🧪 Live scraper", "📈 Reporting", "🗂️ Indexing",
     "📊 Data & export", "🩺 Quality", "🕓 History"])


# ---------------------------------------------------------------------------
# Run tab
# ---------------------------------------------------------------------------
with tab_run:
    st.caption("Each phase has its own screen, live progress, staged-data view and logs. "
               "Flow: ① Courses → ② Colleges/course → ③ Enrichment → ④ Courses & fees · "
               "every run stages → validates → promotes to master.")
    p1, p2, p3, p4, p5 = st.tabs(
        ["①  Courses", "②  Colleges / course", "③  Enrichment", "④  Courses & Fees",
         "⑤  Directory"])

    # ============================ Phase 1 ============================
    with p1:
        st.subheader("Phase 1 — Courses")
        st.info("**Use case:** build the master list of every course Collegedunia lists. "
                "Run **Scrape ALL courses (partitioned)** once to pull the full ~21,500-course "
                "catalogue — it's the foundation Phases 2–4 build on. Outputs the `courses` table.")
        st.caption("Scrapes every course (~21,500). Fast on a clean IP; "
                   "datacenter IPs (incl. Streamlit Cloud) get rate-limited.")
        with st.expander("🚀  One-click full pipeline  (Phase 1 → Phase 2)", expanded=False):
            st.caption("Runs partitioned Phase 1 then Phase 2 with your current settings. "
                       "Proxy must be on.")
            if st.button("🚀 Run full pipeline", type="primary", key="runpipe"):
                cfg = proxy_config_from_ui()
                jid = db.create_job("pipeline", cfg)
                launch_worker(jid)
                st.session_state["watch_p1"] = jid
                st.session_state["watch_p2"] = jid
                st.success(f"Started full pipeline — job #{jid}")
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
            st.session_state["watch_p1"] = jid
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
            st.session_state["watch_p1"] = jid
            st.success(f"Started complete scrape — job #{jid}")
        render_job_monitor(["courses", "pipeline"], "p1")

    # ============================ Phase 2 ============================
    with p2:
        st.subheader("Phase 2 — Colleges per course")
        st.info("**Use case:** for every course, find all colleges that offer it — with fees, "
                "ranking, rating, cutoff and admission dates. Builds the `offerings` and unique "
                "`colleges` tables. The heavy phase: scope it (Top-N or by stream) and keep workers low.")
        st.caption("For each course, scrapes all colleges offering it. Huge: can be hundreds "
                   "of thousands of requests. Scope it! Keep parallel workers at 3–5 to avoid "
                   "proxy rate-limits (403s); one stuck course no longer aborts the run.")
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
        try:
            if scope == "Top N courses by college count":
                ids = cfg2_extra.get("course_ids") or []
                qmark = ",".join("?" * len(ids))
                with db.connect() as conn:
                    slots = conn.execute(
                        f"SELECT COALESCE(SUM(colleges_count),0) FROM courses "
                        f"WHERE course_id IN ({qmark})", ids).fetchone()[0] if ids else 0
            else:
                w = cfg2_extra.get("course_where", "")
                p = cfg2_extra.get("course_where_params", [])
                sql = "SELECT COALESCE(SUM(colleges_count),0) FROM courses"
                if w:
                    sql += f" WHERE {w}"
                with db.connect() as conn:
                    slots = conn.execute(sql, p).fetchone()[0]
            est_pages = (slots + 9) // 10
            est_mb = est_pages * 30 / 1024  # ~30 KB/page
            st.info(f"📊 Forecast: ~{slots:,} college-slots → ~{est_pages:,} requests "
                    f"→ ~**{est_mb:.0f} MB** ({est_mb/1024:.2f} GB). "
                    f"Set a budget cap below if this nears your proxy quota.")
        except Exception:
            pass
        cc1, cc2 = st.columns(2)
        concurrency = cc1.number_input("Parallel workers", 1, 20, 3, key="conc2",
                                       help="Concurrent requests. 3–5 is a good range; higher risks 403 rate-limits.")
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
            st.session_state["watch_p2"] = jid
            st.success(f"Started job #{jid}")
        render_job_monitor(["offerings", "pipeline"], "p2")

    # ============================ Phase 3 ============================
    with p3:
        st.subheader("Phase 3 — College enrichment")
        st.info("**Use case:** fill in each known college's official details — website, email, "
                "phone, rating, pros/cons and address — from its college page. Run after Phase 2; "
                "updates the `colleges` table in place (no staging).")
        st.caption("Fetches each college's page for official website, email, phone, rating, "
                   "pros/cons, and address (no reviews). Pages are ~300 KB each. "
                   "Writes straight to master (no staging step).")
        with db.connect() as conn:
            n_total = conn.execute("SELECT COUNT(*) FROM colleges").fetchone()[0]
            n_done = conn.execute(
                "SELECT COUNT(*) FROM colleges WHERE enriched_at IS NOT NULL").fetchone()[0]
        pending = n_total - n_done
        e1, e2, e3 = st.columns(3)
        e1.metric("Colleges", f"{n_total:,}")
        e2.metric("Enriched", f"{n_done:,}")
        e3.metric("Pending", f"{pending:,}")
        if n_total == 0:
            st.info("Run Phase 2 first so there are colleges to enrich.")
        escope = st.radio("Scope", ["All not-yet-enriched", "Filter by city", "Test (50)"],
                          horizontal=True, key="escope")
        ecfg: dict = {}
        if escope == "Filter by city":
            ecity = st.text_input("City contains", key="ecity")
            if ecity:
                ecfg["college_where"] = "city LIKE ?"
                ecfg["college_where_params"] = [f"%{ecity}%"]
        elif escope == "Test (50)":
            ecfg["limit"] = 50
        target = ecfg.get("limit") or (pending if escope != "Filter by city" else pending)
        est_mb = target * 300 / 1024
        st.info(f"📊 ~{target:,} colleges → ~**{est_mb:.0f} MB** "
                f"({est_mb/1024:.2f} GB) at ~300 KB each. Mind your proxy quota.")
        ec1, ec2 = st.columns(2)
        e_conc = ec1.number_input("Parallel workers", 1, 20, 3, key="econc")
        e_bud = ec2.number_input("Max bandwidth MB (0=∞)", 0, 100000, 0, step=100, key="ebud")
        e_force = st.checkbox("Re-enrich already-done", key="eforce")
        if st.button("🏫 Start college enrichment", key="rune", disabled=n_total == 0):
            cfg = proxy_config_from_ui()
            cfg.update(ecfg)
            cfg["concurrency"] = int(e_conc)
            cfg["budget_mb"] = float(e_bud)
            cfg["force_rescrape"] = e_force
            jid = db.create_job("enrichment", cfg)
            launch_worker(jid)
            st.session_state["watch_p3"] = jid
            st.success(f"Started enrichment — job #{jid}")
        render_job_monitor(["enrichment"], "p3", govern=False)

    # ============================ Phase 4 ============================
    with p4:
        st.subheader("Phase 4 — College courses & fees (college-side)")
        st.info("**Use case:** the most detailed per-college fee view — each college's own "
                "courses-&-fees page: total + hostel fees, eligibility, duration, mode, level, "
                "ratings, application dates and specializations. Fills gaps the course-finder misses.")
        st.caption("Reads each college's /courses-fees page structured data → courses + "
                   "total/hostel fees, duration, eligibility, ratings and application dates. "
                   "~0.3–0.9 MB per college.")
        with db.connect() as conn:
            cc_known = conn.execute("SELECT COUNT(*) FROM colleges").fetchone()[0]
            cc_done = conn.execute(
                "SELECT COUNT(*) FROM cc_progress WHERE status IN ('done','empty')").fetchone()[0]
            cc_rows = conn.execute("SELECT COUNT(*) FROM college_courses").fetchone()[0]
        g1, g2, g3 = st.columns(3)
        g1.metric("Known colleges", f"{cc_known:,}")
        g2.metric("Processed", f"{cc_done:,}")
        g3.metric("Course-rows", f"{cc_rows:,}")
        cscope = st.radio("Source", ["Known colleges (from Phase 2)", "College ID range"],
                          horizontal=True, key="ccscope")
        ccfg: dict = {}
        n_target = max(0, cc_known - cc_done)
        if cscope == "College ID range":
            r1, r2 = st.columns(2)
            ccfg["id_start"] = r1.number_input("ID start", 1, 100000, 1, key="ccs")
            ccfg["id_end"] = r2.number_input("ID end", 1, 100000, 2000, key="cce")
            ccfg["use_known"] = False
            n_target = int(ccfg["id_end"]) - int(ccfg["id_start"]) + 1
        # Each college = 1 SSR page (~0.5 MB) + extra courses-list API pages
        # (~15 KB each). total_pages isn't known until scrape time, so estimate
        # ~2 extra pages/college (page_size 5); actuals are followed via hasNext.
        est_api_pages = n_target * 2
        est_mb = (n_target * 500 + est_api_pages * 15) / 1024
        st.info(f"📊 ~{n_target:,} colleges → ~{n_target + est_api_pages:,} requests "
                f"(1 page each + ~{est_api_pages:,} pagination calls) → ~**{est_mb:.0f} MB** "
                f"({est_mb/1024:.2f} GB), est. Set a budget cap for big ranges.")
        ct1, ct2 = st.columns(2)
        cc_conc = ct1.number_input("Parallel workers", 1, 20, 3, key="cconc")
        cc_bud = ct2.number_input("Max bandwidth MB (0=∞)", 0, 200000, 0, step=200, key="ccbud")
        test4 = st.checkbox("Test run (first 25 known colleges)", key="t4")
        cforce = st.checkbox("Re-scrape already-done", key="ccf")
        if st.button("🏫 Start courses-fees scrape", key="run4",
                     disabled=(cc_known == 0 and cscope.startswith("Known"))):
            cfg = proxy_config_from_ui()
            cfg.update(ccfg)
            if test4:
                cfg["college_ids"] = db.list_known_college_ids()[:25]
                cfg["use_known"] = False
            cfg["concurrency"] = int(cc_conc)
            cfg["budget_mb"] = float(cc_bud)
            cfg["force_rescrape"] = cforce
            jid = db.create_job("college_courses", cfg)
            launch_worker(jid)
            st.session_state["watch_p4"] = jid
            st.success(f"Started courses-fees scrape — job #{jid}")
        if cc_rows > 0:
            with st.expander("🔀 Reconcile: college-side vs course-side"):
                with db.connect() as conn:
                    cc_colleges = conn.execute(
                        "SELECT COUNT(DISTINCT college_id) FROM college_courses").fetchone()[0]
                    off_colleges = conn.execute(
                        "SELECT COUNT(DISTINCT college_id) FROM offerings").fetchone()[0]
                    only_cc = conn.execute(
                        "SELECT COUNT(*) FROM (SELECT DISTINCT college_id FROM college_courses "
                        "WHERE college_id NOT IN (SELECT DISTINCT college_id FROM offerings))").fetchone()[0]
                rc1, rc2, rc3 = st.columns(3)
                rc1.metric("Colleges (Phase 4)", f"{cc_colleges:,}")
                rc2.metric("Colleges (Phase 2)", f"{off_colleges:,}")
                rc3.metric("In Phase 4 only", f"{only_cc:,}",
                           help="Colleges the course-finder never surfaced — the gap this fills.")
        render_job_monitor(["college_courses"], "p4")

    # ============================ Directory ============================
    with p5:
        st.subheader("Directory — full india-colleges baseline")
        st.info("**Use case:** scrape the complete college directory (~20,700 colleges across "
                "35 states) as a **coverage baseline** to find colleges Phase 2 missed. "
                "State-partitioned to beat the listing API's ~999-page ceiling; dedupes on "
                "college_id (tiny states use an HTML fallback). Outputs `colleges_directory`.")
        with db.connect() as conn:
            dir_total = conn.execute("SELECT COUNT(*) FROM colleges_directory").fetchone()[0]
        d1, d2, d3 = st.columns(3)
        d1.metric("Directory colleges", f"{dir_total:,}")
        d2.metric("Phase-2 colleges", f"{c['colleges']:,}")
        d3.metric("Target (approx)", "~20,700")
        st.caption("📊 Budget est.: ~Σ⌈state/10⌉ + 998 base-sweep ≈ **~3,100 requests** "
                   "(~40–60 MB) — cheap vs Phase 2.")
        base_sweep = st.checkbox("Base sweep of india-colleges pages 1–998 (optional, off)",
                                 value=False, key="dirbase",
                                 help="Redundant with the state partitions (which already give "
                                      "full coverage) and tends to hit 403s near the API's "
                                      "~999-page ceiling. Leave OFF unless you want the extra "
                                      "safety net; everything dedupes on college_id.")
        dforce = st.checkbox("Force re-scrape (ignore per-state resume)", key="dirforce")
        if st.button("🗺️ Start directory scrape", type="primary", key="rundir"):
            cfg = proxy_config_from_ui()
            cfg["base_sweep"] = bool(base_sweep)
            cfg["force_rescrape"] = bool(dforce)
            jid = db.create_job("directory", cfg)
            launch_worker(jid)
            st.session_state["watch_p5"] = jid
            st.success(f"Started directory scrape — job #{jid}")
        render_job_monitor(["directory"], "p5")

        st.divider()
        st.markdown("#### 🎯 Coverage vs Phase 2")
        if dir_total == 0:
            st.info("Run the directory scrape first to compare coverage.")
        else:
            cov = db.dir_coverage_summary()
            overlap_pct = (100.0 * cov["overlap"] / cov["directory_total"]) if cov["directory_total"] else 0.0
            missing_total = cov["directory_total"] - cov["overlap"]
            cc1, cc2, cc3 = st.columns(3)
            cc1.metric("Directory total", f"{cov['directory_total']:,}")
            cc2.metric("In Phase 2 (overlap)", f"{cov['overlap']:,}", f"{overlap_pct:.1f}% covered")
            cc3.metric("Missing from Phase 2", f"{missing_total:,}")
            if cov["by_state"]:
                st.markdown("**By state** — directory vs in-Phase-2 vs missing")
                st.dataframe(pd.DataFrame(cov["by_state"]), use_container_width=True, height=280)
            st.markdown("**Missing colleges** (in directory, not in Phase 2)")
            miss = db.dir_missing_from_phase2(limit=2000)
            if miss:
                st.dataframe(pd.DataFrame(miss), use_container_width=True, height=260)
                st.download_button(
                    "⬇️ Download ALL missing (CSV)",
                    data=pd.DataFrame(db.dir_missing_from_phase2(limit=500000))
                         .to_csv(index=False).encode("utf-8"),
                    file_name="directory_missing_from_phase2.csv", mime="text/csv", key="dlmiss")
                if st.button(f"➕ Queue {missing_total:,} missing colleges into Phase 4", key="qmiss"):
                    nq = db.queue_missing_for_phase4()
                    st.success(f"Queued {nq:,} colleges. Run Phase 4 → 'Known colleges' to scrape "
                               "their courses-fees pages.")
            else:
                st.success("✅ No gaps — every directory college is present in Phase 2.")
            with st.expander("🔁 In Phase 2 but NOT in directory (usually fine — flagged)"):
                extra = db.dir_extra_not_in_directory(limit=2000)
                st.dataframe(pd.DataFrame(extra), use_container_width=True, height=220) if extra \
                    else st.caption("None.")


# ---------------------------------------------------------------------------
# Query builder (interactive filters + export)
# ---------------------------------------------------------------------------
with tab_query:
    st.subheader("🔎 Query builder")
    st.info("**Use case:** answer specific questions by filtering offerings / courses / colleges "
            "with live controls (course, college, city, max fee, min rating, max rank), preview "
            "up to 1,000 rows, and export the exact slice as CSV — e.g. *BBA colleges in Pune under ₹2L*.")
    st.caption("Filter the data with live controls, preview, and export the exact slice.")
    base = st.radio("Dataset", ["Offerings (course × college)", "Courses", "Colleges"],
                    horizontal=True, key="qbase")
    where: list = []
    params: list = []

    if base.startswith("Offerings"):
        table = "offerings"
        cols = ("course_name, college_name, city, fees_amount, course_rating, "
                "ranking_rank, ranking_agency, exam_name, university_link")
        x1, x2, x3 = st.columns(3)
        fc = x1.text_input("Course contains", key="qf_c")
        fl = x2.text_input("College contains", key="qf_l")
        fcity = x3.text_input("City contains", key="qf_city")
        y1, y2, y3 = st.columns(3)
        fee_max = y1.number_input("Max 1st-yr fee ₹ (0=any)", 0, 100_000_000, 0, step=50000, key="qf_fee")
        rate_min = y2.number_input("Min rating", 0.0, 5.0, 0.0, step=0.5, key="qf_rate")
        rank_max = y3.number_input("Max rank (0=any)", 0, 100000, 0, step=10, key="qf_rank")
        if fc: where.append("course_name LIKE ?"); params.append(f"%{fc}%")
        if fl: where.append("college_name LIKE ?"); params.append(f"%{fl}%")
        if fcity: where.append("city LIKE ?"); params.append(f"%{fcity}%")
        if fee_max: where.append("fees_amount>0 AND fees_amount<=?"); params.append(int(fee_max))
        if rate_min: where.append("course_rating>=?"); params.append(float(rate_min))
        if rank_max: where.append("ranking_rank>0 AND ranking_rank<=?"); params.append(int(rank_max))
    elif base == "Courses":
        table = "courses"
        cols = "course_id, name, stream_name, course_type, level, eligibility, exam_name, colleges_count"
        fn = st.text_input("Name contains", key="qc_n")
        z1, z2 = st.columns(2)
        sids = z1.multiselect("Streams", [str(s) for s in sorted(db.STREAMS, key=lambda s: db.STREAMS[s])],
                              format_func=lambda s: db.stream_name(s), key="qc_s")
        ctype = z2.multiselect("Course type", ["Degree", "Diploma", "Certification"], key="qc_t")
        if fn: where.append("name LIKE ?"); params.append(f"%{fn}%")
        if sids:
            where.append(f"stream_id IN ({','.join('?'*len(sids))})"); params += sids
        if ctype:
            where.append(f"course_type IN ({','.join('?'*len(ctype))})"); params += ctype
    else:
        table = "colleges"
        cols = ("college_id, name, short_form, city, state_id, website, email, phone, "
                "rating_value, rating_count, address, link")
        fn = st.text_input("Name contains", key="qco_n")
        fcy = st.text_input("City contains", key="qco_c")
        if fn: where.append("name LIKE ?"); params.append(f"%{fn}%")
        if fcy: where.append("city LIKE ?"); params.append(f"%{fcy}%")

    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    with db.connect() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}{wsql}", params).fetchone()[0]
        qdf = pd.read_sql_query(f"SELECT {cols} FROM {table}{wsql} LIMIT 1000", conn, params=params)
    st.caption(f"**{total:,}** match — showing up to 1,000")
    st.dataframe(qdf, use_container_width=True, height=440)

    if st.button("🛠️ Prepare filtered CSV", key="qprep"):
        with st.spinner("Building…"), db.connect() as conn:
            full = pd.read_sql_query(
                f"SELECT {cols} FROM {table}{wsql} LIMIT 200000", conn, params=params)
        st.session_state["qexp"] = full.to_csv(index=False).encode("utf-8")
        if total > 200000:
            st.warning("Capped at 200,000 rows for the download.")
    if st.session_state.get("qexp"):
        st.download_button("⬇️ Download filtered.csv", data=st.session_state["qexp"],
                           file_name="collegedunia_filtered.csv", mime="text/csv")


# ---------------------------------------------------------------------------
# Live scraper — point at any URL, inspect classes, extract by selection
# ---------------------------------------------------------------------------
with tab_live:
    st.subheader("🧪 Live scraper — extract anything by CSS")
    st.info("**Use case:** scrape any page the fixed phases don't cover. Paste a URL, analyze "
            "its structure, pick CSS classes (or type a selector), choose text / text+links / HTML, "
            "and export the result to CSV — a quick general-purpose scraper for one-off pages.")
    st.caption("Enter a page URL, analyze its structure, then pick classes (or type a CSS "
               "selector) and pull the data. Useful for one-off pages the fixed phases don't cover.")
    lc1, lc2 = st.columns([3, 1])
    lurl = lc1.text_input("Page URL", placeholder="https://collegedunia.com/college/...",
                          key="lurl", label_visibility="collapsed")
    use_proxy = lc2.checkbox("Use proxy", value=False, key="luseproxy",
                             help="Fetch via the sidebar proxy/gateway. Off = direct from this server.")
    if st.button("🔍 Analyze page", key="lanalyze", type="primary"):
        if not lurl.strip():
            st.warning("Enter a URL first.")
        else:
            try:
                with st.spinner("Fetching…"):
                    lcfg = proxy_config_from_ui() if use_proxy else {"proxy_mode": "none"}
                    lpm = scraper.ProxyManager.from_config(lcfg)
                    lclient = scraper.Client(lpm, max_retries=int(lcfg.get("max_retries", 3)))
                    lclient.verbose = False
                    lhtml = lclient.get_text(lurl.strip())
                st.session_state["lhtml"] = lhtml
                st.session_state["linfo"] = scraper.analyze_page(lhtml)
                st.session_state.pop("ldf", None)
                st.success(f"Fetched {len(lhtml)//1024} KB · "
                           f"{len(st.session_state['linfo']['classes'])} classes found.")
            except Exception as err:  # noqa: BLE001
                st.error(f"Fetch failed: {str(err)[:200]}")

    info = st.session_state.get("linfo")
    if info:
        if info.get("title"):
            st.caption(f"Page title: {info['title'][:140]}")
        cls = info["classes"]
        if not cls:
            st.info("No CSS classes found on this page. Try a custom selector below.")
        else:
            st.markdown("**Classes found** (most common first — class · count · tags · sample)")
            st.dataframe(pd.DataFrame(cls)[["class", "count", "tags", "sample"]],
                         use_container_width=True, height=260)
        pick = st.multiselect("Pick classes to extract",
                              [c["class"] for c in cls], key="lpick")
        custom = st.text_input("…or a custom CSS selector (overrides the picks above)",
                               placeholder="e.g.  a.college_name   or   div.card h3",
                               key="lcustom")
        mode_label = st.radio("Extract", ["Text only", "Text + links", "Full inner HTML"],
                              horizontal=True, key="lmode")
        mode = {"Text only": "text", "Text + links": "links",
                "Full inner HTML": "html"}[mode_label]
        can_extract = bool(custom.strip() or pick)
        if st.button("📤 Extract", key="lextract", disabled=not can_extract):
            try:
                if custom.strip():
                    rows = scraper.extract_by_selector(
                        st.session_state["lhtml"], custom.strip(), mode=mode)
                else:
                    rows = scraper.extract_by_classes(
                        st.session_state["lhtml"], pick, mode=mode)
                if not rows:
                    st.warning("Nothing matched. Check the class/selector.")
                    st.session_state.pop("ldf", None)
                else:
                    st.session_state["ldf"] = pd.DataFrame(rows)
                    st.success(f"Extracted {len(rows):,} elements.")
            except Exception as err:  # noqa: BLE001
                st.error(f"Extraction failed (bad selector?): {str(err)[:160]}")
        if st.session_state.get("ldf") is not None:
            st.dataframe(st.session_state["ldf"], use_container_width=True, height=360)
            st.download_button(
                "⬇️ Download extracted CSV",
                data=st.session_state["ldf"].to_csv(index=False).encode("utf-8"),
                file_name="live_extract.csv", mime="text/csv", key="ldl")
    else:
        st.info("Enter a URL and click **Analyze page** to begin.")


# ---------------------------------------------------------------------------
# Reporting dashboard (interactive)
# ---------------------------------------------------------------------------
with tab_report:
    st.subheader("📈 Reporting")
    st.info("**Use case:** at-a-glance analytics — courses by stream / type / level, unique colleges "
            "by city, 1st-year fee distribution, top-ranked offerings and geography — plus a one-click "
            "analytics Excel export. Read-only; reflects whatever has been scraped so far.")
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

        if rc["colleges"] > 0:
            st.divider()
            st.markdown("### 🗺️ Geography")
            topc = st.slider("Top N cities", 5, 60, 20, key="geocities")
            gc = agg_colleges_by_city(topc)
            st.markdown("**Colleges by city**")
            st.bar_chart(gc.set_index("k")["n"])
            gs = agg_colleges_by_state()
            if not gs.empty:
                st.markdown("**Colleges by state (state_id)**")
                st.bar_chart(gs.set_index("k")["n"])
                st.caption("State IDs are Collegedunia's internal codes; tell me if "
                           "you want them mapped to state names.")


# ---------------------------------------------------------------------------
# Indexing & coverage dashboard (interactive)
# ---------------------------------------------------------------------------
with tab_index:
    st.subheader("🗂️ Indexing & coverage")
    st.info("**Use case:** track progress and completeness — how many courses are Phase-2 done vs "
            "partial vs pending, per-stream coverage, a searchable index of courses & colleges, and "
            "the biggest still-pending courses to prioritise next.")
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
    st.subheader("📊 Data & export")
    st.info("**Use case:** browse the raw master tables (`courses`, `colleges`, `offerings`, "
            "`college_courses`), quick-filter by name/city, and export the whole table to "
            "CSV / JSON / Excel (single table or all tables in one workbook).")
    table = st.selectbox("Table", ["courses", "colleges", "offerings", "college_courses",
                                   "colleges_directory"])
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
        elif table == "college_courses":
            where = "WHERE course_name LIKE ? OR college_name LIKE ?"
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
with tab_quality:
    st.subheader("🩺 Data quality & maintenance")
    st.info("**Use case:** keep the dataset healthy — normalize text fees to numeric INR, run "
            "health checks (missing fees/ratings/cities, duplicates), export a master analytical "
            "sheet, snapshot dataset size over time, schedule auto-refresh, and reset data "
            "(keep only courses & colleges).")

    st.markdown("**Fee normalization** — parse mixed fee strings into numeric INR.")
    qn1, qn2 = st.columns([1, 2])
    if qn1.button("🔢 Normalize fees now"):
        n = db.normalize_fees()
        st.success(f"Normalized {n} college-course fees into numeric INR (fees_inr).")
    qn2.caption("Offerings already store numeric fees (fees_amount). This fills "
                "college_courses.fees_inr from the text fees.")

    st.divider()
    st.markdown("**Health checks**")
    qa = db.qa_report()
    labels = {
        "courses_zero_colleges": "Courses with 0 colleges",
        "offerings_no_fee": "Offerings missing fee",
        "offerings_no_rating": "Offerings missing rating",
        "colleges_no_city": "Colleges missing city",
        "colleges_unenriched": "Colleges not enriched",
        "dup_college_names": "Duplicate college names",
        "cc_unparsed_fees": "College-courses w/ unparsed fee",
    }
    qcols = st.columns(4)
    for i, (k, lab) in enumerate(labels.items()):
        qcols[i % 4].metric(lab, f"{qa.get(k, 0):,}")

    st.divider()
    st.markdown("**Master export** — one sheet: offerings joined with college details.")
    if st.button("🛠️ Prepare master export"):
        with st.spinner("Building…"):
            st.session_state["masterdata"] = export.to_master_xlsx()
    if st.session_state.get("masterdata"):
        st.download_button("⬇️ Master analytical sheet (.xlsx)",
                           data=st.session_state["masterdata"],
                           file_name="collegedunia_master.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    st.divider()
    st.markdown("**📈 Change log** — snapshots of dataset size over time.")
    if st.button("📸 Take snapshot now"):
        db.add_snapshot("manual")
        st.success("Snapshot saved.")
    snaps = db.get_snapshots(50)
    if snaps:
        sdf = pd.DataFrame([{
            "when": time.strftime("%Y-%m-%d %H:%M", time.localtime(s["ts"])),
            "courses": s["courses"], "colleges": s["colleges"],
            "offerings": s["offerings"], "college_courses": s["college_courses"],
            "note": s.get("note") or "",
        } for s in snaps])
        st.dataframe(sdf, use_container_width=True, height=240)

    st.divider()
    st.markdown("**⏰ Scheduled refresh** — auto re-run on a cadence (always-on host only).")
    sched = db.get_setting("schedule", {}) or {}
    se = st.checkbox("Enable scheduled refresh", value=bool(sched.get("enabled")))
    sc1, sc2 = st.columns(2)
    every_days = sc1.number_input("Every N days", 1, 90, int(sched.get("days", 7)))
    job_type = sc2.selectbox("What to run", ["courses", "pipeline", "enrichment"],
                             index=["courses", "pipeline", "enrichment"].index(sched.get("job_type", "courses")))
    if st.button("💾 Save schedule"):
        nxt = time.time() + (0 if se else 10**12)
        db.set_setting("schedule", {"enabled": bool(se), "days": int(every_days),
                                    "job_type": job_type,
                                    "interval_sec": int(every_days) * 86400,
                                    "next_run": (time.time() + int(every_days) * 86400) if se else 0})
        st.success("Schedule saved." if se else "Scheduling disabled.")
    if sched.get("enabled") and sched.get("next_run"):
        st.caption(f"Next run ≈ {time.strftime('%Y-%m-%d %H:%M', time.localtime(sched['next_run']))}")

    st.divider()
    st.markdown("**🧹 Reset data** — start fresh while keeping your scraped baseline.")
    with db.connect() as conn:
        keep_c = conn.execute("SELECT COUNT(*) FROM courses").fetchone()[0]
        keep_k = conn.execute("SELECT COUNT(*) FROM colleges").fetchone()[0]
    wipe_scope = st.radio(
        "What to keep",
        ["Phase 1 only — courses", "Courses + colleges"],
        key="wipescope",
        help="'Phase 1 only' also clears the colleges table so Phases 2–4 start completely fresh.")
    keep_colleges = wipe_scope.startswith("Courses +")
    if keep_colleges:
        st.caption(f"Will KEEP **{keep_c:,} courses** and **{keep_k:,} colleges** (incl. enrichment) "
                   "and your proxy/settings. Will CLEAR offerings, college-courses, all progress, "
                   "staging, logs, job history and snapshots. Cannot be undone.")
    else:
        st.caption(f"Will KEEP **{keep_c:,} courses** (Phase 1) and your proxy/settings only. "
                   f"Will CLEAR the **{keep_k:,} colleges**, offerings, college-courses, all "
                   "progress, staging, logs, job history and snapshots. Cannot be undone.")
    wipe_ok = st.checkbox("Yes — permanently delete the above", key="wipeok")
    if st.button("🧹 Reset now", disabled=not wipe_ok, key="wipebtn"):
        d = db.wipe_data(keep_colleges=keep_colleges)
        cleared = ", ".join(f"{k}={v:,}" for k, v in d.items() if v)
        st.success("Reset complete. " + (f"Cleared: {cleared}." if cleared
                                         else "Nothing else needed clearing."))
        for _k in ("watch_p1", "watch_p2", "watch_p3", "watch_p4"):
            st.session_state.pop(_k, None)
        st.rerun()


with tab_history:
    st.subheader("🕓 History")
    st.info("**Use case:** every run's status and counters, full logs per job (view / clear / "
            "download), and **job-wise data download** — pull the exact rows a job produced, "
            "either its staged set or what it promoted to master (matched on source_job_id).")
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
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=300)
        if st.button("🔄 Refresh"):
            st.rerun()

        st.divider()
        st.markdown("### 📜 Logs")
        jopts = [j["id"] for j in jobs]
        sel = st.selectbox("View logs for job", jopts,
                           format_func=lambda i: f"#{i}", key="loghist")
        n2 = st.slider("Lines", 50, 1000, 300, key="loghistn")
        hlogs = db.get_logs(sel, limit=int(n2))
        st.code("\n".join(l["message"] for l in reversed(hlogs)) or "(no logs)", language="text")
        d1, d2, d3 = st.columns(3)
        if d1.button("🗑️ Clear this job's logs", key="clr1"):
            db.clear_logs(sel); st.rerun()
        if d2.button("🗑️ Clear ALL logs", key="clrall"):
            db.clear_logs(); st.success("All logs cleared."); st.rerun()
        d3.download_button("⬇️ Download this job's logs",
                           data="\n".join(l["message"] for l in reversed(hlogs)).encode(),
                           file_name=f"job_{sel}_logs.txt", mime="text/plain")

        st.divider()
        st.markdown("### 📦 Download data by job")
        st.caption("Pull the exact rows a job produced — its staged set (before promotion) "
                   "or the rows it wrote to master (matched on source_job_id).")
        dj = st.selectbox("Job", jopts, format_func=lambda i: f"#{i}", key="dljob")
        dstaged = db.staged_summary(dj)
        promoted = {}
        with db.connect() as conn:
            for _t in ("courses", "colleges", "offerings", "college_courses"):
                try:
                    promoted[_t] = conn.execute(
                        f"SELECT COUNT(*) FROM {_t} WHERE source_job_id=?", (dj,)).fetchone()[0]
                except Exception:
                    promoted[_t] = 0
        promoted = {k: v for k, v in promoted.items() if v}
        if not dstaged and not promoted:
            st.info("No data recorded for this job yet (it may still be running, have written "
                    "straight to master before provenance tracking, or produced no rows).")
        else:
            src_opts = (["Staged (this job)"] if dstaged else []) + \
                       (["Promoted to master"] if promoted else [])
            src = st.radio("Source", src_opts, horizontal=True, key="dlsrc")
            if src.startswith("Staged"):
                dtbl = st.selectbox("Table", list(dstaged.keys()), key="dlstbl")
                st.caption(f"{dstaged[dtbl]:,} staged rows")
                if st.button("🛠️ Prepare CSV", key="dlsprep"):
                    with st.spinner("Building…"):
                        rows = db.get_staged_rows(dj, dtbl, limit=500000)
                        st.session_state["jobdl"] = (
                            pd.DataFrame(rows).to_csv(index=False).encode("utf-8"),
                            f"job{dj}_{dtbl}_staged.csv")
            else:
                dtbl = st.selectbox("Table", list(promoted.keys()), key="dlptbl")
                st.caption(f"{promoted[dtbl]:,} rows in master from this job")
                if st.button("🛠️ Prepare CSV", key="dlpprep"):
                    with st.spinner("Building…"), db.connect() as conn:
                        jdf = pd.read_sql_query(
                            f"SELECT * FROM {dtbl} WHERE source_job_id=? LIMIT 500000",
                            conn, params=[dj])
                    st.session_state["jobdl"] = (
                        jdf.to_csv(index=False).encode("utf-8"), f"job{dj}_{dtbl}.csv")
            if st.session_state.get("jobdl"):
                _data, _fn = st.session_state["jobdl"]
                st.download_button(f"⬇️ Download {_fn}", data=_data, file_name=_fn,
                                   mime="text/csv", key="jobdlbtn")
