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
import cf_export
import cf_vertical  # noqa: F401  registers 'coursefinder'
_VERTICALS = (cf_vertical,)   # referenced so pyflakes keeps the registration import
import db as _core

SECS_PER_REQ = 19.0     # observed round-trip through the proxy

# A download costs ~2x its size in server RAM (Streamlit holds one copy in
# session_state and another to serve). On a 2 GB box that is what killed the
# domestic export before it was fixed, so build to a temp file first and refuse
# anything over the cap rather than OOM the host and log the user out.
MAX_DL_MB = float(os.environ.get("CD_MAX_DOWNLOAD_MB", "150"))


def _prepare(build, suffix):
    """Build to a temp file, then load it only if it is under the cap.
    Returns (bytes | None, size); None means 'too large to serve'."""
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
                f"payload*, or use **.xlsx** — it is zip-compressed and typically "
                f"far smaller.")
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


def _estimate_mb(table: str, include_raw: bool) -> float:
    """Rough size before building, so the user isn't surprised."""
    try:
        with cf_db.connect() as conn:
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

        # ------------------------------------------------------------ export
        st.divider()
        st.markdown("### ⬇️ Export")
        raw = st.checkbox(
            "Include raw payload (`raw_json`)", value=False, key="cfe_raw",
            help="Off by default — the payloads are the bulk of the data and every "
                 "parsed column is exported either way.")
        st.caption(f"Builds to a temp file first, so a large export can't blow up "
                   f"the server. Anything over {MAX_DL_MB:.0f} MB is refused rather "
                   f"than served.")

        e1, e2, e3 = st.columns(3)
        est_t = _estimate_mb(which, raw)
        e1.caption(f"`{which}` ≈ {est_t:,.1f} MB")

        if e1.button(f"🛠️ Build {which}.csv", key="cfe_csv"):
            with st.spinner("Building CSV…"):
                d, s = _prepare(
                    lambda p: cf_export.to_csv(which, include_raw=raw, out_path=p),
                    ".csv")
            _offer(st, d, s, f"{which}.csv", "cfe_csv_data")
        _render_pending(st, "cfe_csv_data", "text/csv")

        if e2.button(f"🛠️ Build {which}.json", key="cfe_json"):
            with st.spinner("Building JSON…"):
                d, s = _prepare(
                    lambda p: cf_export.to_json(which, include_raw=raw, out_path=p),
                    ".json")
            _offer(st, d, s, f"{which}.json", "cfe_json_data")
        _render_pending(st, "cfe_json_data", "application/json")

        if e3.button("🛠️ Build course_finder.xlsx (all tables)", key="cfe_xlsx"):
            with st.spinner("Building workbook…"):
                d, s = _prepare(
                    lambda p: cf_export.to_xlsx(include_raw=raw, out_path=p),
                    ".xlsx")
            _offer(st, d, s, "course_finder.xlsx", "cfe_xlsx_data")
        _render_pending(st, "cfe_xlsx_data",
                        "application/vnd.openxmlformats-officedocument."
                        "spreadsheetml.sheet")

        with cf_db.connect() as conn:
            n_off = conn.execute("SELECT COUNT(*) FROM cf_offerings").fetchone()[0]
        if n_off > cf_export.XLSX_MAX_ROWS:
            st.warning(
                f"`cf_offerings` has {n_off:,} rows — past Excel's "
                f"{cf_export.XLSX_MAX_ROWS:,}-row sheet limit. The .xlsx export "
                f"truncates that sheet; use CSV for the complete table.",
                icon="⚠️")

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
