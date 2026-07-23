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
import shutil
import subprocess
import sys
import threading
import time

import pandas as pd
import streamlit as st

import db
import export
import scraper

# Build stamp — bumped whenever the matched set of core files changes. The header
# cross-checks db/scraper/export against this value; if you deploy a stale subset,
# a banner names the out-of-sync file instead of failing with a cryptic 404.
BUILD = "2026-07-23a"


def _build_status():
    mods = {"app.py": BUILD, "db.py": getattr(db, "BUILD", "?"),
            "scraper.py": getattr(scraper, "BUILD", "?"),
            "export.py": getattr(export, "BUILD", "?")}
    stale = {k: v for k, v in mods.items() if v != BUILD}
    return mods, stale

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


def _preview_df(df):
    """Make a dataframe light/safe to render in Streamlit's data grid: drop the
    heavy raw_json blob and truncate long text cells. Very large cells can make
    the grid loop ('Maximum update depth exceeded' / React #185). Exports use the
    full data separately — this only affects on-screen previews."""
    try:
        drop = [c for c in ("raw_json",) if c in df.columns]
        out = df.drop(columns=drop) if drop else df
        for col in out.columns:
            if out[col].dtype == object:
                out[col] = out[col].astype(str).str.slice(0, 300)
        return out
    except Exception:
        return df


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


# --- Phase 4 (college_courses) aggregations ---------------------------------
@st.cache_data(ttl=25, show_spinner=False)
def agg_cc_summary() -> dict:
    with db.connect() as conn:
        def one(q):
            return conn.execute(q).fetchone()[0]
        return {
            "rows": one("SELECT COUNT(*) FROM college_courses"),
            "colleges": one("SELECT COUNT(DISTINCT college_id) FROM college_courses"),
            "with_fee": one("SELECT COUNT(*) FROM college_courses WHERE fees_inr>0"),
            "with_hostel": one("SELECT COUNT(*) FROM college_courses "
                               "WHERE hostel_fees IS NOT NULL AND hostel_fees<>''"),
            "processed": one("SELECT COUNT(*) FROM cc_progress WHERE status IN ('done','empty')"),
        }


@st.cache_data(ttl=25, show_spinner=False)
def agg_cc_fee_buckets() -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT CASE "
            "WHEN fees_inr<=25000 THEN '0-25k' WHEN fees_inr<=50000 THEN '25-50k' "
            "WHEN fees_inr<=100000 THEN '50k-1L' WHEN fees_inr<=200000 THEN '1-2L' "
            "WHEN fees_inr<=500000 THEN '2-5L' WHEN fees_inr<=1000000 THEN '5-10L' "
            "ELSE '10L+' END AS k, COUNT(*) AS n, MIN(fees_inr) AS mn "
            "FROM college_courses WHERE fees_inr>0 GROUP BY k ORDER BY mn", conn)


@st.cache_data(ttl=25, show_spinner=False)
def agg_cc_group(col: str) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            f"SELECT COALESCE(NULLIF(TRIM({col}),''),'—') AS k, COUNT(*) AS n "
            f"FROM college_courses GROUP BY k ORDER BY n DESC LIMIT 20", conn)


@st.cache_data(ttl=25, show_spinner=False)
def agg_cc_top_colleges(n: int) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT college_name AS k, COUNT(*) AS n, "
            "SUM(CASE WHEN fees_inr>0 THEN 1 ELSE 0 END) AS with_fee "
            "FROM college_courses WHERE college_name<>'' "
            "GROUP BY college_id ORDER BY n DESC LIMIT ?", conn, params=[int(n)])


# --- Directory (colleges_directory) aggregations ----------------------------
@st.cache_data(ttl=25, show_spinner=False)
def agg_dir_summary() -> dict:
    with db.connect() as conn:
        def one(q):
            return conn.execute(q).fetchone()[0]
        return {
            "total": one("SELECT COUNT(*) FROM colleges_directory"),
            "states": one("SELECT COUNT(DISTINCT state) FROM colleges_directory "
                          "WHERE state IS NOT NULL AND state<>''"),
            "cities": one("SELECT COUNT(DISTINCT city) FROM colleges_directory "
                          "WHERE city IS NOT NULL AND city<>''"),
            "rated": one("SELECT COUNT(*) FROM colleges_directory WHERE rating>0"),
        }


@st.cache_data(ttl=25, show_spinner=False)
def agg_dir_by_state(n: int) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT COALESCE(NULLIF(state,''),'?') AS k, COUNT(*) AS n "
            "FROM colleges_directory GROUP BY state ORDER BY n DESC LIMIT ?",
            conn, params=[int(n)])


@st.cache_data(ttl=25, show_spinner=False)
def agg_dir_by_city(n: int) -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT COALESCE(NULLIF(city,''),'?') AS k, COUNT(*) AS n "
            "FROM colleges_directory GROUP BY city ORDER BY n DESC LIMIT ?",
            conn, params=[int(n)])


@st.cache_data(ttl=25, show_spinner=False)
def agg_dir_rating_buckets() -> pd.DataFrame:
    with db.connect() as conn:
        return pd.read_sql_query(
            "SELECT CASE "
            "WHEN rating<2 THEN '<2' WHEN rating<3 THEN '2-3' WHEN rating<4 THEN '3-4' "
            "WHEN rating<4.5 THEN '4-4.5' ELSE '4.5-5' END AS k, COUNT(*) AS n, "
            "MIN(rating) AS mn FROM colleges_directory WHERE rating>0 "
            "GROUP BY k ORDER BY mn", conn)


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


def fmt_ago(ts) -> str:
    if not ts:
        return "—"
    secs = max(0, time.time() - float(ts))
    if secs < 90:
        return "just now"
    mins = secs / 60
    if mins < 90:
        return f"{mins:.0f} min ago"
    hrs = mins / 60
    if hrs < 36:
        return f"{hrs:.0f} hr ago"
    return f"{hrs/24:.0f} d ago"


# ---------------------------------------------------------------------------
# System metrics (container-aware, stdlib only — no extra deps)
# ---------------------------------------------------------------------------
@st.cache_resource
def _proc_start_time() -> float:
    return time.time()


def _read_int(path: str):
    try:
        with open(path) as fh:
            return int(fh.read().split()[0])
    except Exception:
        return None


def _proc_rss_bytes():
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        pass
    return None


def _cgroup_mem():
    """Container memory (used, limit) in bytes — cgroup v2 then v1."""
    lim = _read_int("/sys/fs/cgroup/memory.max")
    use = _read_int("/sys/fs/cgroup/memory.current")
    if lim is None:
        lim = _read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        use = _read_int("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    if lim and lim > (1 << 62):          # 'max' / unlimited sentinel
        lim = None
    if use is None or lim is None:       # fallback: /proc/meminfo (host-level)
        mi = {}
        try:
            with open("/proc/meminfo") as fh:
                for line in fh:
                    parts = line.split(":")
                    if len(parts) == 2:
                        mi[parts[0]] = int(parts[1].split()[0]) * 1024
        except Exception:
            pass
        if lim is None:
            lim = mi.get("MemTotal")
        if use is None and mi.get("MemTotal") and "MemAvailable" in mi:
            use = mi["MemTotal"] - mi["MemAvailable"]
    return use, lim


def _cgroup_cpu_usec():
    """Cumulative CPU time used by the container, in microseconds."""
    try:
        with open("/sys/fs/cgroup/cpu.stat") as fh:
            for line in fh:
                if line.startswith("usage_usec"):
                    return int(line.split()[1])
    except Exception:
        pass
    ns = _read_int("/sys/fs/cgroup/cpuacct/cpuacct.usage")
    return ns // 1000 if ns else None


def _cpu_percent(interval: float = 0.15):
    c1 = _cgroup_cpu_usec()
    if c1 is None:
        return None
    t1 = time.time()
    time.sleep(interval)
    c2 = _cgroup_cpu_usec()
    dt = time.time() - t1
    if c2 is None or dt <= 0:
        return None
    return max(0.0, 100.0 * (c2 - c1) / (dt * 1e6) / (os.cpu_count() or 1))


def _fmt_bytes(n):
    if n is None:
        return "—"
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"


def _fmt_dur(s):
    s = int(s or 0)
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    if h:
        return f"{h}h {m}m"
    if m:
        return f"{m}m {sec}s"
    return f"{sec}s"


def render_system_bar() -> None:
    """A live panel of container/system metrics — CPU, memory (cgroup limit),
    process RSS, disk, DB size, load, threads, active jobs, uptime."""
    now = time.time()
    cpu = _cpu_percent()
    mem_use, mem_lim = _cgroup_mem()
    mem_pct = (100.0 * mem_use / mem_lim) if (mem_use and mem_lim) else None
    rss = _proc_rss_bytes()
    try:
        du = shutil.disk_usage(os.path.dirname(db.DB_PATH) or "/")
        disk_used, disk_total = du.used, du.total
        disk_pct = 100.0 * du.used / du.total if du.total else None
    except Exception:
        disk_used = disk_total = disk_pct = None
    try:
        db_size = os.path.getsize(db.DB_PATH)
    except Exception:
        db_size = None
    try:
        la1, la5, la15 = os.getloadavg()
    except Exception:
        la1 = la5 = la15 = None
    try:
        jobs = db.list_jobs(40)
        running = sum(1 for j in jobs if j["status"] in ("running", "queued"))
    except Exception:
        running = 0
    threads = threading.active_count()
    cores = os.cpu_count() or 1
    uptime = now - _proc_start_time()

    with st.container(border=True):
        st.caption("🖥️ System — live (refreshes on every rerun; live during a running job)")
        r1 = st.columns(6)
        r1[0].metric("CPU", f"{cpu:.0f}%" if cpu is not None else "—", f"{cores} cores")
        r1[1].metric("Memory", f"{mem_pct:.0f}%" if mem_pct is not None else "—",
                     f"{_fmt_bytes(mem_use)} / {_fmt_bytes(mem_lim)}")
        r1[2].metric("App RSS", _fmt_bytes(rss))
        r1[3].metric("Disk", f"{disk_pct:.0f}%" if disk_pct is not None else "—",
                     f"{_fmt_bytes(disk_used)} / {_fmt_bytes(disk_total)}")
        r1[4].metric("DB size", _fmt_bytes(db_size))
        r1[5].metric("Load 1m", f"{la1:.2f}" if la1 is not None else "—",
                     f"5m {la5:.2f}" if la5 is not None else None)
        r2 = st.columns(6)
        r2[0].metric("Active jobs", f"{running}")
        r2[1].metric("Threads", f"{threads}")
        r2[2].metric("Load 15m", f"{la15:.2f}" if la15 is not None else "—")
        r2[3].metric("Uptime", _fmt_dur(uptime))
        r2[4].metric("Mem free", _fmt_bytes((mem_lim - mem_use) if (mem_lim and mem_use) else None))
        r2[5].metric("Disk free", _fmt_bytes((disk_total - disk_used) if (disk_total and disk_used) else None))


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
_bmods, _bstale = _build_status()
if _bstale:
    st.error(
        f"⚠️ **Version mismatch** — app.py is build `{BUILD}` but these files are "
        f"out of sync: {', '.join(f'{k} = {v}' for k, v in _bstale.items())}. "
        "Re-upload the **full matched set** (app.py, db.py, scraper.py, export.py) "
        "and redeploy with *Clear build cache*. This is the usual cause of 404 "
        "exports and the 'Connecting' / SessionInfo errors.")
else:
    st.caption(f"🧩 Build `{BUILD}` · core files in sync (app · db · scraper · export)")
c = db.counts()
m1, m2, m3, m4 = st.columns(4)
m1.metric("Courses", f"{c['courses']:,}")
m2.metric("Unique colleges", f"{c['colleges']:,}")
m3.metric("Offerings (course×college)", f"{c['offerings']:,}")
m4.metric("Courses done (phase 2)", f"{c['courses_done_phase2']:,}")

render_system_bar()


@st.fragment(run_every=3)
def render_job_monitor(job_types, key: str, govern: bool = True) -> None:
    """Per-phase live monitor: progress, staged data (live), logs, controls —
    scoped to this phase's job (its watch id, else the latest job of job_types).

    Wrapped in an st.fragment so the 3-second live refresh reruns ONLY this panel,
    not the whole app. The old approach (time.sleep + st.rerun) re-ran every tab and
    the system bar each cycle, hammering the websocket and causing Render to flash
    'CONNECTING' / 'Tried to use SessionInfo before it was initialized'."""
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
                    st.dataframe(_preview_df(pd.DataFrame(srows)), use_container_width=True, height=300)
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
    lc2.caption(f"{len(logs)} lines shown · auto-refreshes every 3s while running")
    # Live refresh is handled by the @st.fragment(run_every=3) decorator — no
    # blocking sleep / full-app st.rerun() here (that caused the Render websocket
    # churn). The fragment quietly re-renders just this panel every 3 seconds.


(tab_overview, tab_run, tab_query, tab_live, tab_report, tab_index, tab_data,
 tab_quality, tab_history) = st.tabs(
    ["🗄️ Overview", "▶️ Run", "🔎 Query", "🧪 Live scraper", "📈 Reporting",
     "🗂️ Indexing", "📊 Data & export", "🩺 Quality", "🕓 History"])


with tab_overview:
    st.subheader("🗄️ Database overview")
    st.info("**Use case:** see everything you've scraped in one place — every table with its "
            "row count and description, plus per-column completeness, a sample, and Phase-2 vs "
            "directory coverage. Read-only; nothing is written here.")
    try:
        _dbsize = os.path.getsize(db.DB_PATH)
    except Exception:
        _dbsize = None
    st.caption(f"SQLite database · {_fmt_bytes(_dbsize)}")

    _TABLE_INFO = {
        "courses": "Phase 1 — course catalogue",
        "colleges": "Phase 2 — unique colleges (Phase 3 enriches these)",
        "offerings": "Phase 2 — course × college (fees, ranking, cutoff, dates)",
        "college_courses": "Phase 4 — per-college courses & fees",
        "colleges_directory": "Directory — full india-colleges baseline",
        "offering_progress": "Phase 2 progress (per course)",
        "cc_progress": "Phase 4 progress (per college)",
        "dir_progress": "Directory progress (per state)",
        "jobs": "Scrape run history",
        "staging": "Un-promoted staged rows (pending governance)",
        "snapshots": "Dataset-size snapshots",
        "logs": "Live logs",
    }
    _rows = []
    with db.connect() as conn:
        for _t, _desc in _TABLE_INFO.items():
            try:
                _n = conn.execute(f"SELECT COUNT(*) FROM {_t}").fetchone()[0]
            except Exception:
                _n = None
            _rows.append({"table": _t, "rows": _n, "what it is": _desc})
    st.markdown("**Tables**")
    st.dataframe(pd.DataFrame(_rows), use_container_width=True, height=440)

    _staged = next((r["rows"] for r in _rows if r["table"] == "staging"), 0) or 0
    if _staged:
        st.warning(f"⚠️ **{_staged:,} rows are sitting in `staging`** (scraped but not yet in the "
                   "master tables) — a backlog from jobs that were interrupted before promoting. "
                   "Promote them into master (upsert — dedupes against existing rows) or discard.")
        _pc = st.columns(2)
        if _pc[0].button(f"🚚 Promote ALL staged → master ({_staged:,} rows)", key="flushall",
                         type="primary"):
            with st.spinner("Promoting in memory-safe chunks…"):
                _summ = db.flush_all_staging()
            st.success("Promoted → " + (", ".join(f"{k}={v:,}" for k, v in _summ.items())
                                        if _summ else "nothing to promote."))
            st.rerun()
        if _pc[1].button("🗑️ Discard ALL staged", key="discardall"):
            _nd = db.discard_all_staging()
            st.warning(f"Discarded {_nd:,} staged rows.")
            st.rerun()

    _dtot = next((r["rows"] for r in _rows if r["table"] == "colleges_directory"), 0) or 0
    if _dtot:
        _cov = db.dir_coverage_summary()
        _pct = (100.0 * _cov["overlap"] / _cov["directory_total"]) if _cov["directory_total"] else 0.0
        st.markdown("**Coverage — directory vs Phase 2**")
        _o = st.columns(3)
        _o[0].metric("Directory colleges", f"{_cov['directory_total']:,}")
        _o[1].metric("In Phase 2 (overlap)", f"{_cov['overlap']:,}", f"{_pct:.1f}% covered")
        _o[2].metric("Missing from Phase 2", f"{_cov['directory_total'] - _cov['overlap']:,}")

    st.divider()
    st.markdown("**🧹 Junk / quality audit**")
    try:
        _junk = db.count_junk_college_courses()
    except Exception:
        _junk = 0
    try:
        _qa = db.qa_report()
    except Exception:
        _qa = {}
    _q = st.columns(4)
    _q[0].metric("Junk course-rows", f"{_junk:,}",
                 help="college_courses rows whose name is a fee label / amount / college "
                      "name — leftovers from the old parser. Not true duplicates, but junk.")
    _q[1].metric("Offerings w/o fee", f"{_qa.get('offerings_no_fee', 0):,}")
    _q[2].metric("Duplicate college names", f"{_qa.get('dup_college_names', 0):,}",
                 help="Same name, different college_id — usually separate campuses, not dupes.")
    _q[3].metric("Colleges not enriched", f"{_qa.get('colleges_unenriched', 0):,}")
    if _junk:
        with st.expander(f"Preview junk rows (up to 50 of {_junk:,})"):
            st.dataframe(pd.DataFrame(db.sample_junk_college_courses(50)),
                         use_container_width=True, height=260)
        _delok = st.checkbox("Yes, delete these junk course-rows from college_courses",
                             key="junkok")
        if st.button(f"🧹 Delete {_junk:,} junk rows", key="deljunk", disabled=not _delok):
            _dn = db.delete_junk_college_courses()
            st.success(f"Deleted {_dn:,} junk rows from college_courses. Re-running Phase 4 "
                       "only adds clean rows, so they won't come back.")
            st.rerun()
    else:
        st.success("✅ No junk course-rows detected in college_courses.")

    st.divider()
    st.markdown("**Inspect a table** — columns, completeness (sampled) and a data sample")
    _pick = st.selectbox("Table", [r["table"] for r in _rows if r["rows"]], key="ovtbl")
    if _pick:
        with db.connect() as conn:
            _cols = [r[1] for r in conn.execute(f"PRAGMA table_info({_pick})")]
            _total = conn.execute(f"SELECT COUNT(*) FROM {_pick}").fetchone()[0]
            _base = f"(SELECT * FROM {_pick} LIMIT 5000)"
            _sn = conn.execute(f"SELECT COUNT(*) FROM {_base}").fetchone()[0] or 1
            _comp = []
            for _c in _cols:
                try:
                    _f = conn.execute(
                        f"SELECT COUNT(*) FROM {_base} WHERE {_c} IS NOT NULL "
                        f"AND CAST({_c} AS TEXT)<>''").fetchone()[0]
                except Exception:
                    _f = conn.execute(
                        f"SELECT COUNT(*) FROM {_base} WHERE {_c} IS NOT NULL").fetchone()[0]
                _comp.append({"column": _c, "filled": _f,
                              "% filled": round(100.0 * _f / _sn, 1)})
            _sample = pd.read_sql_query(f"SELECT * FROM {_pick} LIMIT 20", conn)
        _m = st.columns(2)
        _m[0].metric("Rows", f"{_total:,}")
        _m[1].metric("Columns", f"{len(_cols)}")
        st.markdown(f"**Column completeness** (of a {_sn:,}-row sample)")
        st.dataframe(pd.DataFrame(_comp), use_container_width=True, height=300)
        st.markdown("**Sample (first 20 rows)**")
        st.dataframe(_preview_df(_sample), use_container_width=True, height=320)


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

        # ---- Enrichment B: backfill empty fields ---------------------------
        with st.expander("✨ Enrichment B — backfill empty course fields", expanded=False):
            with db.connect() as conn:
                blanks = conn.execute(
                    "SELECT "
                    "SUM(CASE WHEN duration IS NULL OR duration='' THEN 1 ELSE 0 END) AS d, "
                    "SUM(CASE WHEN eligibility IS NULL OR eligibility='' THEN 1 ELSE 0 END) AS e, "
                    "SUM(CASE WHEN avg_salary IS NULL OR avg_salary='' THEN 1 ELSE 0 END) AS s, "
                    "SUM(CASE WHEN job_roles IS NULL OR job_roles='' THEN 1 ELSE 0 END) AS j, "
                    "COUNT(*) AS n FROM courses").fetchone()
            st.caption(
                "Re-runs the light Phase-1 listing and patches **only blank** fields on "
                "existing courses (never overwrites good data, adds no rows). Writes straight "
                "to master. Uses the partitioned scrape so it reaches every course.")
            if blanks and blanks["n"]:
                bb1, bb2, bb3, bb4 = st.columns(4)
                bb1.metric("Missing duration", f"{blanks['d'] or 0:,}")
                bb2.metric("Missing eligibility", f"{blanks['e'] or 0:,}")
                bb3.metric("Missing salary", f"{blanks['s'] or 0:,}")
                bb4.metric("Missing job roles", f"{blanks['j'] or 0:,}")
            bf_part = st.checkbox("Partitioned (reach all ~21.5k courses)", value=True,
                                  key="bf_part")
            if st.button("✨ Start backfill (empty fields only)", key="run1bf"):
                cfg = proxy_config_from_ui()
                cfg["backfill_only"] = True
                cfg["staging"] = False           # patch master directly
                cfg["force_restart"] = True       # sweep the whole catalogue
                if bf_part:
                    cfg["partition"] = True
                jid = db.create_job("courses", cfg)
                launch_worker(jid)
                st.session_state["watch_p1"] = jid
                st.success(f"Started backfill — job #{jid}. Only empty fields will be filled.")
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
        with db.connect() as conn:
            dir_total = conn.execute(
                "SELECT COUNT(*) FROM colleges_directory "
                "WHERE college_id IS NOT NULL").fetchone()[0]
        cscope = st.radio(
            "Source",
            ["Known colleges (from Phase 2)",
             f"All directory colleges ({dir_total:,})",
             "College ID range"],
            horizontal=True, key="ccscope")
        ccfg: dict = {}
        n_target = max(0, cc_known - cc_done)
        if cscope.startswith("All directory"):
            n_target = max(0, dir_total - cc_done)
            ccfg["use_directory"] = True
            ccfg["use_known"] = False
            st.caption(
                f"Scrapes courses & fees for **all {dir_total:,}** directory colleges. "
                f"Resume skips the {cc_done:,} already processed, so this run fills the "
                f"**~{n_target:,} pending** — including the ~{max(0, dir_total - cc_known):,} "
                "colleges Phase 2 never found. Set a bandwidth cap and let it run in chunks.")
        elif cscope == "College ID range":
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
        fetch_hostel = st.checkbox(
            "Also fetch hostel fee (slower: +~0.6 MB HTML page per college)",
            value=False, key="cchostel",
            help="Hostel fee isn't in the fast JSON API. Leave OFF for a much faster, "
                 "lighter run (fewer 403s) — you only lose the hostel_fees column; "
                 "everything else (fees, eligibility, duration, ratings, dates) still comes through.")
        if st.button("🏫 Start courses-fees scrape", key="run4",
                     disabled=(cc_known == 0 and cscope.startswith("Known"))):
            cfg = proxy_config_from_ui()
            cfg.update(ccfg)
            if cscope.startswith("All directory"):
                cfg["college_ids"] = db.list_directory_college_ids()
                cfg["use_known"] = False
            if test4:
                cfg["college_ids"] = db.list_known_college_ids()[:25]
                cfg["use_known"] = False
            cfg["concurrency"] = int(cc_conc)
            cfg["budget_mb"] = float(cc_bud)
            cfg["force_rescrape"] = cforce
            cfg["fetch_hostel"] = bool(fetch_hostel)
            jid = db.create_job("college_courses", cfg)
            launch_worker(jid)
            st.session_state["watch_p4"] = jid
            st.success(f"Started courses-fees scrape — job #{jid}")

        # ---- Resumable course-URL backfill on already-scraped colleges -----
        if cc_rows > 0:
            with st.expander("🔗 Backfill course URLs on already-scraped colleges",
                             expanded=False):
                ub = db.course_url_backfill_status()
                st.caption(
                    "Re-scrapes only colleges whose existing course rows are missing a "
                    "`course_url`, refilling every column (incl. the new URL) via the fast "
                    "courses-list API. **Resumable**: each college drops out of the queue once "
                    "filled, so you can run it in chunks with a bandwidth cap and just restart.")
                ubm1, ubm2, ubm3 = st.columns(3)
                ubm1.metric("Course-rows total", f"{ub['rows_total']:,}")
                ubm2.metric("Rows missing URL", f"{ub['rows_missing']:,}")
                ubm3.metric("Colleges to backfill", f"{ub['colleges_missing']:,}")
                ub_est = ub["colleges_missing"] * 25 / 1024
                st.caption(f"~{ub['colleges_missing']:,} colleges → ~**{ub_est:.0f} MB** "
                           "via the light JSON API (~25 KB each). Set a cap and run in chunks.")
                ubc1, ubc2 = st.columns(2)
                ub_conc = ubc1.number_input("Parallel workers", 1, 20, 3, key="ubconc")
                ub_bud = ubc2.number_input("Max bandwidth MB (0=∞)", 0, 200000, 500,
                                           step=100, key="ubbud")
                if st.button("🔗 Start URL backfill", key="run4url",
                             disabled=ub["colleges_missing"] == 0):
                    ids = db.list_colleges_missing_course_url()
                    cfg = proxy_config_from_ui()
                    cfg["college_ids"] = ids
                    cfg["use_known"] = False
                    cfg["force_rescrape"] = True      # re-hit 'done' colleges
                    cfg["fetch_hostel"] = False       # URL comes from the JSON API
                    cfg["concurrency"] = int(ub_conc)
                    cfg["budget_mb"] = float(ub_bud)
                    jid = db.create_job("college_courses", cfg)
                    launch_worker(jid)
                    st.session_state["watch_p4"] = jid
                    st.success(f"Started URL backfill over {len(ids):,} colleges — job #{jid}. "
                               "Restart after a budget stop; it auto-continues.")

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
                if extra:
                    st.dataframe(pd.DataFrame(extra), use_container_width=True, height=220)
                else:
                    st.caption("None.")


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
    st.info("**Use case:** at-a-glance analytics across **every phase** — courses, Phase-2 "
            "offerings, Phase-4 college courses & fees, and the full directory — with live filters "
            "and a one-click analytics Excel export. Read-only; reflects whatever's scraped so far.")
    rc = db.counts()
    ccs = agg_cc_summary()
    dirs = agg_dir_summary()
    try:
        stg = db.staging_count()
    except Exception:  # noqa: BLE001
        stg = 0

    # ---- Headline KPIs across all phases -----------------------------------
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Courses (P1)", f"{rc['courses']:,}")
    k2.metric("Colleges (P2)", f"{rc['colleges']:,}")
    k3.metric("Offerings (P2)", f"{rc['offerings']:,}")
    k4.metric("College-courses (P4)", f"{ccs['rows']:,}")
    k5.metric("Directory colleges", f"{dirs['total']:,}")
    k6.metric("Staged (unpromoted)", f"{stg:,}")

    if rc["courses"] == 0 and ccs["rows"] == 0 and dirs["total"] == 0:
        st.info("No data yet — run a scrape first.")
    else:
        ecol1, ecol2 = st.columns([1, 3])
        with ecol1:
            if st.button("🛠️ Prepare analytics export", use_container_width=True):
                with st.spinner("Building…"):
                    st.session_state["andata"] = export.to_analytics_xlsx()
        with ecol2:
            if st.session_state.get("andata"):
                st.download_button(
                    "⬇️ Analytics summary (.xlsx)", data=st.session_state["andata"],
                    file_name="collegedunia_analytics.xlsx", use_container_width=True,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        r_p1, r_p2, r_p4, r_dir, r_geo = st.tabs(
            ["🎓 Courses (P1)", "🏫 Offerings (P2)", "💸 College courses (P4)",
             "🗂️ Directory", "🗺️ Geography"])

        # ---- Phase 1: courses ----------------------------------------------
        with r_p1:
            if rc["courses"] == 0:
                st.caption("Run Phase 1 to populate the course catalogue.")
            else:
                bs = agg_group("stream_name").rename(columns={"k": "stream"}).set_index("stream")["n"]
                bt = agg_group("course_type").rename(columns={"k": "type"}).set_index("type")["n"]
                bl = agg_group("level").rename(columns={"k": "level"}).set_index("level")["n"]
                a, b = st.columns(2)
                with a:
                    st.markdown("**Courses by stream**"); st.bar_chart(bs, height=260)
                with b:
                    st.markdown("**Courses by type**"); st.bar_chart(bt, height=260)
                st.markdown("**Courses by level**"); st.bar_chart(bl, height=240)
                st.divider()
                # ---- Enrichment A: derived per-course aggregates ------------
                ce = db.course_enrichment_summary()
                st.markdown("**✨ Course enrichment** — fee ranges, reach & top colleges "
                            "derived from your Phase-2 offerings (no new scraping)")
                eca, ecb = st.columns([1, 2])
                with eca:
                    if st.button("⚡ Build / refresh enrichment", key="rep_p1_enrich",
                                 disabled=rc["offerings"] == 0, use_container_width=True):
                        with st.spinner("Aggregating offerings…"):
                            written = db.enrich_courses()
                        st.success(f"Enriched {written:,} courses.")
                        ce = db.course_enrichment_summary()
                with ecb:
                    if ce["rows"]:
                        st.caption(f"Enriched **{ce['rows']:,}** courses"
                                   + (f" · updated {fmt_ago(ce['last'])}" if ce.get("last") else ""))
                    elif rc["offerings"] == 0:
                        st.caption("Run Phase 2 first — enrichment is derived from offerings.")
                    else:
                        st.caption("Not built yet — click the button to derive it.")

                st.divider()
                st.markdown("**🔎 Courses — with enrichment** — filter by stream")
                streams = ["(all)"] + [s for s in bs.index.tolist() if s]
                fstream = st.selectbox("Stream", streams, key="rep_p1_stream")
                only_en = st.checkbox("Only enriched courses", value=False, key="rep_p1_onlyen")
                topk = st.slider("Show top", 10, 200, 40, step=10, key="rep_p1_topk")
                join = "LEFT JOIN" if not only_en else "JOIN"
                q = ("SELECT c.course_id, c.name, c.stream_name, c.level, c.colleges_count, "
                     "e.n_colleges AS scraped_colleges, e.fee_min, e.fee_avg, e.fee_max, "
                     "e.n_states, e.avg_rating "
                     f"FROM courses c {join} course_enrichment e ON e.course_id=c.course_id")
                pr: list = []
                if fstream != "(all)":
                    q += " WHERE c.stream_name=?"; pr = [fstream]
                q += " ORDER BY c.colleges_count DESC LIMIT ?"; pr.append(int(topk))
                with db.connect() as conn:
                    dfp1 = pd.read_sql_query(q, conn, params=pr)
                st.dataframe(dfp1, use_container_width=True, height=340)
                # Drill-down: top colleges + state spread for one course
                if ce["rows"] and not dfp1.empty:
                    pick = st.selectbox(
                        "Drill into a course (top colleges & geography)",
                        ["—"] + dfp1["name"].tolist(), key="rep_p1_pick")
                    if pick and pick != "—":
                        cid = int(dfp1[dfp1["name"] == pick]["course_id"].iloc[0])
                        with db.connect() as conn:
                            row = conn.execute(
                                "SELECT top_colleges, state_spread FROM course_enrichment "
                                "WHERE course_id=?", (cid,)).fetchone()
                        if row:
                            import json as _json
                            tops = _json.loads(row["top_colleges"] or "[]")
                            if tops:
                                st.markdown("**Top colleges**")
                                st.dataframe(pd.DataFrame(tops), use_container_width=True,
                                             height=210)
                            spread = _json.loads(row["state_spread"] or "{}")
                            if spread:
                                st.markdown("**Colleges by state_id**")
                                st.bar_chart(pd.Series(spread), height=200)

        # ---- Phase 2: offerings --------------------------------------------
        with r_p2:
            if rc["offerings"] == 0:
                st.caption("Run Phase 2 to unlock college / fee / ranking analytics.")
            else:
                topn = st.slider("Top N cities", 5, 50, 15, key="rep_p2_cities")
                cc = agg_city_counts(topn).set_index("k")["n"]
                st.markdown("**Unique colleges by city**"); st.bar_chart(cc, height=260)
                fb = agg_fee_buckets()
                if not fb.empty:
                    st.markdown("**1st-year fee distribution (₹)**")
                    st.bar_chart(fb.set_index("k")["n"], height=240)
                tr = agg_top_ranked()
                if not tr.empty:
                    st.markdown("**Top-ranked offerings**")
                    cityf = st.text_input("Filter by city / college", key="rep_p2_f")
                    show = tr
                    if cityf:
                        m = (tr["city"].str.contains(cityf, case=False, na=False) |
                             tr["college_name"].str.contains(cityf, case=False, na=False))
                        show = tr[m]
                    st.dataframe(show, use_container_width=True, height=320)

        # ---- Phase 4: college courses & fees -------------------------------
        with r_p4:
            if ccs["rows"] == 0:
                st.caption("Run Phase 4 (college courses & fees) to populate this section.")
            else:
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Course-rows", f"{ccs['rows']:,}")
                m2.metric("Colleges covered", f"{ccs['colleges']:,}")
                m3.metric("Rows with fee", f"{ccs['with_fee']:,}")
                m4.metric("Rows with hostel fee", f"{ccs['with_hostel']:,}")
                fbc = agg_cc_fee_buckets()
                if not fbc.empty:
                    st.markdown("**Total-fee distribution (₹)**")
                    st.bar_chart(fbc.set_index("k")["n"], height=240)
                cca, ccb = st.columns(2)
                lv = agg_cc_group("level")
                md = agg_cc_group("mode")
                with cca:
                    st.markdown("**By level**"); st.bar_chart(lv.set_index("k")["n"], height=240)
                with ccb:
                    st.markdown("**By mode**"); st.bar_chart(md.set_index("k")["n"], height=240)
                st.divider()
                st.markdown("**🏆 Colleges with the most courses**")
                tcn = st.slider("Top N colleges", 5, 60, 20, key="rep_p4_top")
                tc = agg_cc_top_colleges(tcn)
                st.bar_chart(tc.set_index("k")["n"], height=260)
                st.divider()
                st.markdown("**🔎 Browse course rows** — filter by fee & level")
                f1, f2, f3 = st.columns(3)
                fmin = f1.number_input("Min fee ₹", 0, 5000000, 0, step=10000, key="rep_p4_fmin")
                fmax = f2.number_input("Max fee ₹ (0=∞)", 0, 5000000, 0, step=10000, key="rep_p4_fmax")
                levels = ["(all)"] + [x for x in lv["k"].tolist() if x and x != "—"]
                flev = f3.selectbox("Level", levels, key="rep_p4_lev")
                q = ("SELECT college_name, course_name, level, mode, duration, "
                     "total_fees, hostel_fees, fees_inr, course_url "
                     "FROM college_courses WHERE 1=1")
                pr = []
                if fmin > 0:
                    q += " AND fees_inr>=?"; pr.append(int(fmin))
                if fmax > 0:
                    q += " AND fees_inr<=?"; pr.append(int(fmax))
                if flev != "(all)":
                    q += " AND level=?"; pr.append(flev)
                q += " ORDER BY fees_inr DESC LIMIT 500"
                with db.connect() as conn:
                    dcc = pd.read_sql_query(q, conn, params=pr)
                st.caption(f"{len(dcc):,} shown (max 500)")
                st.dataframe(dcc, use_container_width=True, height=360)

        # ---- Directory -----------------------------------------------------
        with r_dir:
            if dirs["total"] == 0:
                st.caption("Run the Directory phase to populate the coverage baseline.")
            else:
                d1, d2, d3, d4 = st.columns(4)
                d1.metric("Directory colleges", f"{dirs['total']:,}")
                d2.metric("States", f"{dirs['states']:,}")
                d3.metric("Cities", f"{dirs['cities']:,}")
                d4.metric("Rated", f"{dirs['rated']:,}")
                dtn = st.slider("Top N states", 5, 40, 20, key="rep_dir_states")
                st.markdown("**Colleges by state**")
                st.bar_chart(agg_dir_by_state(dtn).set_index("k")["n"], height=280)
                dca, dcb = st.columns(2)
                with dca:
                    st.markdown("**Colleges by city (top 20)**")
                    st.bar_chart(agg_dir_by_city(20).set_index("k")["n"], height=260)
                with dcb:
                    rbk = agg_dir_rating_buckets()
                    if not rbk.empty:
                        st.markdown("**Rating distribution**")
                        st.bar_chart(rbk.set_index("k")["n"], height=260)
                st.divider()
                st.markdown("**🔎 Browse directory** — filter by state & search")
                with db.connect() as conn:
                    stopts = ["(all)"] + [r[0] for r in conn.execute(
                        "SELECT DISTINCT state FROM colleges_directory "
                        "WHERE state IS NOT NULL AND state<>'' ORDER BY state")]
                fc1, fc2 = st.columns(2)
                fst = fc1.selectbox("State", stopts, key="rep_dir_state")
                fterm = fc2.text_input("Search name / city", key="rep_dir_term")
                q = ("SELECT college_id, name, city, state, rating, naac_grading, "
                     "course_count, top_course_fees, approvals FROM colleges_directory WHERE 1=1")
                pr = []
                if fst != "(all)":
                    q += " AND state=?"; pr.append(fst)
                if fterm:
                    q += " AND (name LIKE ? OR city LIKE ?)"; pr += [f"%{fterm}%", f"%{fterm}%"]
                q += " ORDER BY rating DESC, name LIMIT 500"
                with db.connect() as conn:
                    ddf = pd.read_sql_query(q, conn, params=pr)
                st.caption(f"{len(ddf):,} shown (max 500)")
                st.dataframe(ddf, use_container_width=True, height=360)

        # ---- Geography (master colleges) -----------------------------------
        with r_geo:
            if rc["colleges"] == 0:
                st.caption("Run Phase 2 to populate master colleges.")
            else:
                topc = st.slider("Top N cities", 5, 60, 20, key="geocities")
                st.markdown("**Colleges by city (Phase 2 master)**")
                st.bar_chart(agg_colleges_by_city(topc).set_index("k")["n"], height=260)
                gs = agg_colleges_by_state()
                if not gs.empty:
                    st.markdown("**Colleges by state (state_id)**")
                    st.bar_chart(gs.set_index("k")["n"], height=240)
                    st.caption("Phase-2 master uses Collegedunia's internal state_id codes. "
                               "The Directory tab has real state names.")


# ---------------------------------------------------------------------------
# Indexing & coverage dashboard (interactive)
# ---------------------------------------------------------------------------
with tab_index:
    st.subheader("🗂️ Indexing & coverage")
    st.info("**Use case:** track progress and completeness for **every phase** — Phase-2 offerings, "
            "Phase-4 college courses, and Directory — plus directory-vs-scraped coverage, a unified "
            "searchable index across all four datasets, and the biggest still-pending work to prioritise.")
    ic = db.counts()
    with db.connect() as conn:
        def _one(q):
            return conn.execute(q).fetchone()[0]
        p2_done = _one("SELECT COUNT(*) FROM offering_progress WHERE status='done'")
        p2_partial = _one("SELECT COUNT(*) FROM offering_progress WHERE status='partial'")
        cc_done = _one("SELECT COUNT(*) FROM cc_progress WHERE status IN ('done','empty')")
        cc_queued = _one("SELECT COUNT(*) FROM cc_progress WHERE status='queued'")
        dir_total = _one("SELECT COUNT(*) FROM colleges_directory")
        dir_slugs_done = _one("SELECT COUNT(*) FROM dir_progress WHERE status='done'")
        dir_slugs_part = _one("SELECT COUNT(*) FROM dir_progress WHERE status='partial'")
    total_courses = ic["courses"]

    prog2, prog4, progd = st.tabs(
        ["🏫 Phase 2 — offerings", "💸 Phase 4 — college courses", "🗂️ Directory"])

    # ---- Phase 2 progress --------------------------------------------------
    with prog2:
        p2_pending = max(0, total_courses - p2_done - p2_partial)
        i1, i2, i3, i4 = st.columns(4)
        i1.metric("Courses indexed", f"{total_courses:,}")
        i2.metric("Done", f"{p2_done:,}")
        i3.metric("Partial", f"{p2_partial:,}")
        i4.metric("Pending", f"{p2_pending:,}")
        if total_courses:
            st.progress(p2_done / total_courses,
                        text=f"Phase-2 coverage: {p2_done:,}/{total_courses:,} courses done")
            cov = agg_coverage()
            if not cov.empty:
                cov["done"] = cov["done"].fillna(0).astype(int)
                cov["% done"] = (cov["done"] / cov["courses"] * 100).round(1)
                st.markdown("**Coverage by stream**")
                st.dataframe(cov, use_container_width=True, height=280)
            st.markdown("**Biggest still-pending courses** — filter by stream")
            with db.connect() as conn:
                strms = ["(all)"] + [r[0] for r in conn.execute(
                    "SELECT DISTINCT stream_name FROM courses "
                    "WHERE stream_name IS NOT NULL AND stream_name<>'' ORDER BY stream_name")]
            pstream = st.selectbox("Stream", strms, key="idx_p2_stream")
            q = ("SELECT c.course_id, c.name, c.stream_name, c.colleges_count "
                 "FROM courses c LEFT JOIN offering_progress op ON c.course_id=op.course_id "
                 "WHERE (op.course_id IS NULL OR op.status<>'done')")
            pr = []
            if pstream != "(all)":
                q += " AND c.stream_name=?"; pr = [pstream]
            q += " ORDER BY c.colleges_count DESC LIMIT 300"
            with db.connect() as conn:
                st.dataframe(pd.read_sql_query(q, conn, params=pr),
                             use_container_width=True, height=300)

    # ---- Phase 4 progress --------------------------------------------------
    with prog4:
        cc_rows = agg_cc_summary()["rows"]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Colleges processed", f"{cc_done:,}")
        c2.metric("Queued (from gap)", f"{cc_queued:,}")
        c3.metric("Course-rows", f"{cc_rows:,}")
        c4.metric("Directory baseline", f"{dir_total:,}")
        if dir_total:
            st.progress(min(1.0, cc_done / dir_total),
                        text=f"Phase-4 vs directory: {cc_done:,}/{dir_total:,} colleges processed "
                             f"({cc_done/dir_total*100:.0f}%)")
            st.caption(f"~{max(0, dir_total - cc_done):,} directory colleges still have no Phase-4 "
                       "courses/fees. Run Phase 4 → Source: **All directory colleges** to close the gap.")
        st.markdown("**Directory colleges still missing Phase-4 data** — biggest first")
        with db.connect() as conn:
            miss = pd.read_sql_query(
                "SELECT d.college_id, d.name, d.state, d.course_count, d.rating "
                "FROM colleges_directory d "
                "LEFT JOIN cc_progress p ON p.college_id=d.college_id "
                "WHERE p.college_id IS NULL OR p.status NOT IN ('done','empty') "
                "ORDER BY d.course_count DESC LIMIT 300", conn)
        st.caption(f"{len(miss):,} shown (max 300)")
        st.dataframe(miss, use_container_width=True, height=300)

    # ---- Directory progress ------------------------------------------------
    with progd:
        d1, d2, d3 = st.columns(3)
        d1.metric("Directory colleges", f"{dir_total:,}")
        d2.metric("Partitions done", f"{dir_slugs_done:,}")
        d3.metric("Partitions partial", f"{dir_slugs_part:,}")
        cov = db.dir_coverage_summary()
        st.progress(
            min(1.0, (cov["overlap"] / cov["directory_total"]) if cov["directory_total"] else 0),
            text=f"In Phase 2 as well: {cov['overlap']:,}/{cov['directory_total']:,} directory colleges")
        by_state = pd.DataFrame(cov["by_state"])
        if not by_state.empty:
            st.markdown("**Coverage by state — directory vs Phase 2 (missing = gap to fill)**")
            only_missing = st.checkbox("Show only states with gaps", value=False, key="idx_dir_gap")
            show = by_state[by_state["missing"] > 0] if only_missing else by_state
            st.dataframe(show, use_container_width=True, height=320)

    # ---- Unified browse index ---------------------------------------------
    st.divider()
    st.markdown("### 🔎 Unified index — search across every dataset")
    which = st.radio(
        "Dataset",
        ["Courses (P1)", "Colleges (P2)", "College courses (P4)", "Directory"],
        horizontal=True, key="idx_which")
    term = st.text_input("Search by name / city", key="idxsearch")
    like = f"%{term}%"
    with db.connect() as conn:
        if which == "Courses (P1)":
            sql = ("SELECT course_id, name, stream_name, course_type, level, colleges_count "
                   "FROM courses")
            params = []
            if term:
                sql += " WHERE name LIKE ?"; params = [like]
            sql += " ORDER BY colleges_count DESC LIMIT 500"
        elif which == "Colleges (P2)":
            sql = ("SELECT college_id, name, short_form, city, state_id, rating_value, "
                   "enriched_at FROM colleges")
            params = []
            if term:
                sql += " WHERE name LIKE ? OR city LIKE ?"; params = [like, like]
            sql += " ORDER BY name LIMIT 500"
        elif which == "College courses (P4)":
            sql = ("SELECT college_name, course_name, level, mode, duration, "
                   "total_fees, hostel_fees, course_url FROM college_courses")
            params = []
            if term:
                sql += " WHERE college_name LIKE ? OR course_name LIKE ?"; params = [like, like]
            sql += " ORDER BY fees_inr DESC LIMIT 500"
        else:  # Directory
            sql = ("SELECT college_id, name, city, state, rating, naac_grading, "
                   "course_count, approvals FROM colleges_directory")
            params = []
            if term:
                sql += " WHERE name LIKE ? OR city LIKE ?"; params = [like, like]
            sql += " ORDER BY rating DESC, name LIMIT 500"
        idf = pd.read_sql_query(sql, conn, params=params)
    st.caption(f"{len(idf):,} shown (max 500)")
    st.dataframe(idf, use_container_width=True, height=360)


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
    st.dataframe(_preview_df(df), use_container_width=True, height=420)
    st.caption("Preview hides the large `raw_json` column and truncates long text — "
               "downloads below include the full data.")

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
