"""
Study Abroad — UI module rendered INSIDE the shared app (app.py) behind a vertical
switch. Uses the SHARED database but only `sa_` tables. Scrapes still run as their
own detached worker processes (platform_worker.py), so a failing SA scrape cannot
kill a domestic scrape. Exposes a single `render()` the host app calls.
"""
from __future__ import annotations

BUILD = "2026-07-23a"

import os
import subprocess
import sys

import pandas as pd
import streamlit as st

import vertical_base as vb
import sa_vertical  # noqa: F401  registers 'studyabroad'
import sa_db


def _launch_worker(vertical: str, job_id: int) -> None:
    try:
        subprocess.Popen([sys.executable, "platform_worker.py", vertical, str(job_id)],
                         start_new_session=True,
                         cwd=os.path.dirname(os.path.abspath(__file__)))
    except Exception as e:  # noqa: BLE001
        st.error(f"Failed to launch worker: {e}")


def _proxy_cfg(gw: str, budget_mb: float, delay: float) -> dict:
    cfg = {"budget_mb": float(budget_mb), "delay": float(delay)}
    if gw.strip():
        cfg.update({"proxies": [gw.strip()], "proxy_mode": "gateway", "gateway_url": gw.strip()})
    return cfg


def render() -> None:
    """Entry point called by app.py when the Study Abroad vertical is selected."""
    V = vb.get("studyabroad")
    V.init_db()  # creates sa_ tables in the shared data.db (IF NOT EXISTS)

    st.title(V.label)
    st.caption(f"{V.description}  ·  shared DB, isolated `sa_` tables  ·  build `{BUILD}`")

    # --- sidebar proxy/limits (SA-scoped) ---
    st.sidebar.markdown("### 🌍 Study Abroad settings")
    gw = st.sidebar.text_input("Proxy gateway URL (blank = direct)",
                               value=sa_db.get_setting("proxy_gateway", "") or "", key="sa_gw")
    if gw != (sa_db.get_setting("proxy_gateway", "") or ""):
        sa_db.set_setting("proxy_gateway", gw)
    budget_mb = st.sidebar.number_input("Budget MB/run (0=∞)", 0, 200000, 0, step=100, key="sa_bud")
    delay = st.sidebar.number_input("Delay between pages (s)", 0.0, 30.0, 1.0, step=0.5, key="sa_delay")

    t_over, t_run, t_data, t_hist = st.tabs(
        ["🗄️ Overview", "▶️ Run", "📊 Data & export", "🕓 History"])

    # ---------- Overview ----------
    with t_over:
        c = V.counts()
        cols = st.columns(min(len(c), 4) or 1)
        for i, (k, v) in enumerate(c.items()):
            cols[i % len(cols)].metric(k.replace("_", " ").title(), f"{v:,}")
        st.caption(f"Tables `sa_*` in the shared database `{os.path.basename(V.db_path)}`. "
                   "Study Abroad never reads or writes domestic tables.")

    # ---------- Run ----------
    with t_run:
        for ph in V.phases:
            with st.container(border=True):
                st.markdown(f"**{ph.label}** — {ph.description}")
                dep = f" · depends on: {', '.join(ph.depends_on)}" if ph.depends_on else ""
                st.caption(f"id: `{ph.id}`{dep}")
                if st.button(f"▶️ Start {ph.label}", key=f"sa_start_{ph.id}"):
                    jid = sa_db.create_job(ph.id, _proxy_cfg(gw, budget_mb, delay))
                    _launch_worker(V.name, jid)
                    st.session_state["sa_watch"] = jid
                    st.success(f"Started {ph.label} — job #{jid}")

        st.divider()

        @st.fragment(run_every=3)
        def _monitor():
            jid = st.session_state.get("sa_watch")
            jobs = sa_db.list_jobs(1)
            job = sa_db.get_job(jid) if jid else (jobs[0] if jobs else None)
            st.markdown("##### 📡 Live progress & logs")
            if not job:
                st.info("No runs yet. Start ① Facets, then ② Programs.")
                return
            total = job.get("total_units") or 0
            done = job.get("done_units") or 0
            st.progress(min(done / total, 1.0) if total else 0.0,
                        text=f"Job #{job['id']} ({job['phase']}) — {job['status']}")
            m = st.columns(4)
            m[0].metric("Written", f"{job.get('items_written') or 0:,}")
            m[1].metric("Status", job["status"])
            m[2].metric("Phase", job["phase"])
            m[3].metric("Job", f"#{job['id']}")
            st.caption(job.get("message") or "")
            if job["status"] in ("running", "queued"):
                if st.button("⏹️ Stop", key=f"sa_stop{job['id']}"):
                    sa_db.request_stop(job["id"])
                    st.warning("Stop requested.")
            logs = sa_db.get_logs(job["id"], 150)
            st.code("\n".join(l["message"] for l in reversed(logs)) or "(waiting for logs…)",
                    language="text")

        _monitor()

    # ---------- Data & export ----------
    with t_data:
        with sa_db.connect(V.db_path) as conn:
            tbls = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sa_%' "
                "ORDER BY name")]
        tbl = st.selectbox("Table (sa_ only)", tbls, key="sa_tbl")
        term = st.text_input("Search name / university", key="sa_search")
        if tbl:
            q = f"SELECT * FROM {tbl}"
            params = []
            if term and tbl == "sa_programs":
                q += " WHERE name LIKE ? OR university_name LIKE ?"
                params = [f"%{term}%", f"%{term}%"]
            q += " LIMIT 500"
            with sa_db.connect(V.db_path) as conn:
                df = pd.read_sql_query(q, conn, params=params)
            for col in ("raw_json", "description"):
                if col in df.columns:
                    df = df.drop(columns=[col])
            st.caption(f"{len(df):,} shown (max 500)")
            st.dataframe(df, use_container_width=True, height=380)
        if V.export_xlsx and st.button("🛠️ Build .xlsx (SA only)", key="sa_xlsx_btn"):
            with st.spinner("Building…"):
                st.session_state["sa_xlsx"] = V.export_xlsx()
        if st.session_state.get("sa_xlsx"):
            st.download_button("⬇️ Download SA .xlsx", data=st.session_state["sa_xlsx"],
                               file_name="study_abroad_export.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ---------- History ----------
    with t_hist:
        jobs = sa_db.list_jobs(50)
        if jobs:
            jdf = pd.DataFrame(jobs)[["id", "phase", "status", "items_written", "message"]]
            st.dataframe(jdf, use_container_width=True, height=420)
        else:
            st.info("No jobs yet.")
