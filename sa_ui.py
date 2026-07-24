"""
Study Abroad — full UI section rendered INSIDE app.py behind the vertical switch.
Mirrors the domestic tab set (Overview, Run, Query, Live scraper, Reporting,
Indexing, Data & export, Quality, History) but reads/writes ONLY `sa_` tables in
the shared database. Uses the SAME proxy/gateway the domestic app has configured.
Scrapes run as detached worker processes (platform_worker.py).
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
import sa_scraper
import db as _core  # to display the SHARED proxy config (read-only)
import scraper as _eng  # reuse analyze_page/extract_* for the Live scraper


def _launch_worker(vertical: str, job_id: int) -> None:
    try:
        subprocess.Popen([sys.executable, "platform_worker.py", vertical, str(job_id)],
                         start_new_session=True,
                         cwd=os.path.dirname(os.path.abspath(__file__)))
    except Exception as e:  # noqa: BLE001
        st.error(f"Failed to launch worker: {e}")


@st.cache_data(ttl=20, show_spinner=False)
def _agg(sql: str, params=()):
    with sa_db.connect() as conn:
        return pd.read_sql_query(sql, conn, params=params)


def _shared_proxy_line() -> str:
    mode = _core.get_setting("proxy_mode", "none")
    gw = _core.get_setting("proxy_gateway", "") or ""
    if mode == "gateway" and gw:
        host = gw.split("@")[-1]
        return f"gateway → `{host}`"
    if mode == "list":
        n = len([p for p in (_core.get_setting("proxy_list_text", "") or "").splitlines() if p.strip()])
        return f"list ({n} proxies)"
    return "direct (no proxy)"


def render() -> None:
    V = vb.get("studyabroad")
    V.init_db()

    st.title(V.label)
    st.caption(f"{V.description} · shared DB, isolated `sa_` tables · uses domestic proxy as-is "
               f"({_shared_proxy_line()}) · build `{BUILD}`")

    (t_over, t_run, t_query, t_live, t_report, t_index, t_data, t_quality, t_hist) = st.tabs(
        ["🗄️ Overview", "▶️ Run", "🔎 Query", "🧪 Live scraper", "📈 Reporting",
         "🗂️ Indexing", "📊 Data & export", "🩺 Quality", "🕓 History"])

    # ============================= Overview =============================
    with t_over:
        st.info("**Use case:** headline counts + coverage for the Study Abroad program "
                "dataset. Read-only; reflects whatever has been promoted to `sa_` tables so far.")
        c = V.counts()
        row1 = st.columns(4)
        row1[0].metric("Programs", f"{c['programs']:,}")
        row1[1].metric("Universities", f"{c['universities']:,}")
        row1[2].metric("Countries", f"{c['countries']:,}")
        row1[3].metric("Exam requirements", f"{c['program_exams']:,}")
        row2 = st.columns(4)
        row2[0].metric("With program URL", f"{c['programs_with_url']:,}")
        row2[1].metric("With fee (+currency)", f"{c['programs_with_fee']:,}")
        row2[2].metric("Distinct currencies", f"{c['distinct_currencies']:,}")
        row2[3].metric("Staged (unpromoted)", f"{sa_db.staging_count():,}")
        total = _core.get_setting("total_programs", None)
        if total:
            st.progress(min(c["programs"] / int(total), 1.0),
                        text=f"Coverage: {c['programs']:,} / ~{int(total):,} programs on the site")
        st.caption(f"All data in `sa_*` tables inside the shared `{os.path.basename(V.db_path)}`. "
                   "Study Abroad never reads or writes domestic tables.")

    # ============================= Run =============================
    with t_run:
        st.info("**Use case:** run the Study Abroad scraping phases with the SAME engine as domestic "
                "— adaptive throttling, the configured proxy/gateway, budgets, and the "
                "staging→validate→promote governance pipeline.")
        with st.expander("⚙️ Governance & limits", expanded=True):
            g1, g2, g3 = st.columns(3)
            staging = g1.checkbox("Stage → validate → promote", value=True, key="sa_stg")
            auto_promote = g2.checkbox("Auto-promote when QC passes", value=True, key="sa_auto")
            incremental = g3.checkbox("Incremental promote (memory-safe)", value=True, key="sa_inc")
            b1, b2, b3 = st.columns(3)
            budget_mb = b1.number_input("Bandwidth budget MB (0=∞)", 0, 200000, 0, step=100, key="sa_bmb")
            budget_reqs = b2.number_input("Request budget (0=∞)", 0, 5000000, 0, step=1000, key="sa_breq")
            concurrency = b3.number_input("Parallel workers", 1, 20, 1, step=1, key="sa_conc",
                                          help="Parallel leaf-partition crawlers. SA rate-limiting is "
                                               "lenient; 3–5 is a safe speed-up. 1 = sequential.")
            q1, q2, q3 = st.columns(3)
            v_min = q1.number_input("QC min rows", 0, 1000000, 1, key="sa_vmin")
            v_miss = q2.number_input("QC max missing-fee %", 0, 100, 100, key="sa_vmiss")
            v_pass = q3.number_input("QC pass score", 0, 100, 70, key="sa_vpass")
            st.caption(f"Proxy/gateway (shared with domestic, used as-is): **{_shared_proxy_line()}** · "
                       "adaptive throttle is ON. Change proxy in the domestic sidebar settings.")

        total = _core.get_setting("total_programs", None)
        if total:
            pages = int(int(total) / 20) + 1
            st.caption(f"📊 Forecast: ~{int(total):,} programs → ~{pages:,} page requests "
                       f"→ ~{pages * 0.05:.0f} MB (rough, ~50 KB/page). Partitioned under the ~10k cap.")

        def _cfg():
            return {
                "budget_mb": float(budget_mb), "budget_requests": int(budget_reqs),
                "concurrency": int(concurrency), "adaptive": True, "staging": bool(staging),
                "auto_promote": bool(auto_promote), "incremental_promote": bool(incremental),
                "validation_rules": {"min_rows": int(v_min), "max_missing_fee_pct": float(v_miss),
                                     "pass_score": float(v_pass)},
            }

        for ph in V.phases:
            with st.container(border=True):
                st.markdown(f"**{ph.label}** — {ph.description}")
                dep = f" · depends on: {', '.join(ph.depends_on)}" if ph.depends_on else ""
                st.caption(f"id: `{ph.id}`{dep}")
                if st.button(f"▶️ Start {ph.label}", key=f"sa_start_{ph.id}"):
                    jid = sa_db.create_job(ph.id, _cfg())
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
            m = st.columns(5)
            m[0].metric("Written", f"{job.get('items_written') or 0:,}")
            m[1].metric("Requests", f"{job.get('req_count') or 0:,}")
            m[2].metric("Bandwidth", f"{(job.get('bytes_count') or 0)/1048576:.1f} MB")
            qs = job.get("quality_score")
            m[3].metric("Quality", f"{qs:.0f}/100" if qs is not None else "—")
            m[4].metric("Status", job["status"])
            st.caption(job.get("message") or "")
            staged = sa_db.staged_summary(job["id"])
            if staged:
                st.caption("🔬 Staged (live): " + ", ".join(f"{k}={v:,}" for k, v in staged.items()))
                finished = job["status"] not in ("running", "queued")
                if finished and sum(staged.values()) > 0:
                    a1, a2 = st.columns(2)
                    if a1.button(f"✅ Promote staged ({sum(staged.values()):,})", key=f"sa_pr{job['id']}"):
                        summ = sa_db.promote_job(job["id"])
                        st.success(f"Promoted {sum(summ.values()):,} rows.")
                    if a2.button("🗑️ Discard staged", key=f"sa_ds{job['id']}"):
                        sa_db.discard_staging(job["id"])
                        st.warning("Staged data discarded.")
            if job["status"] in ("running", "queued"):
                if st.button("⏹️ Stop", key=f"sa_stop{job['id']}"):
                    sa_db.request_stop(job["id"])
                    st.warning("Stop requested.")
            if job["status"] in ("stopped", "error"):
                if st.button("▶️ Resume job", key=f"sa_res{job['id']}"):
                    sa_db.update_job(job["id"], status="queued", stop_requested=0)
                    _launch_worker(V.name, job["id"])  # resumes from partition progress
                    st.success("Resumed — continues from saved partition progress.")
            logs = sa_db.get_logs(job["id"], 150)
            st.code("\n".join(l["message"] for l in reversed(logs)) or "(waiting for logs…)",
                    language="text")

        _monitor()

    # ============================= Query =============================
    with t_query:
        st.info("**Use case:** filter and browse the program dataset by country, degree tag, "
                "attendance mode, and fee.")
        _cty = _agg("SELECT DISTINCT country_code FROM sa_programs "
                    "WHERE country_code<>'' ORDER BY country_code")
        countries = ["(all)"] + (_cty["country_code"].tolist() if not _cty.empty else [])
        f1, f2, f3 = st.columns(3)
        fc = f1.selectbox("Country", countries, key="saq_country")
        ftag = f2.text_input("Degree/tag contains (e.g. MBA)", key="saq_tag")
        fmax = f3.number_input("Max fee (native, 0=∞)", 0, 100000000, 0, step=1000, key="saq_fee")
        term = st.text_input("Search program / university name", key="saq_term")
        q = ("SELECT program_id, name, university_name, country_code, course_tags, program_type, "
             "duration_text, fee_currency, fee_native_amount, fee_inr_amount, program_url "
             "FROM sa_programs WHERE 1=1")
        pr = []
        if fc != "(all)":
            q += " AND country_code=?"; pr.append(fc)
        if ftag:
            q += " AND course_tags LIKE ?"; pr.append(f"%{ftag}%")
        if fmax:
            q += " AND fee_native_amount<=?"; pr.append(int(fmax))
        if term:
            q += " AND (name LIKE ? OR university_name LIKE ?)"; pr += [f"%{term}%", f"%{term}%"]
        q += " ORDER BY fee_inr_amount DESC LIMIT 500"
        df = _agg(q, tuple(pr))
        st.caption(f"{len(df):,} shown (max 500)")
        st.dataframe(df, use_container_width=True, height=400)

        with st.expander("🧬 Custom SQL (read-only, sa_ tables)"):
            sql = st.text_area("SELECT … FROM sa_programs …", key="sa_sql", height=90)
            if st.button("Run query", key="sa_sql_run") and sql.strip():
                s = sql.strip().rstrip(";")
                if ";" in s or not s.lower().lstrip().startswith("select"):
                    st.error("Only a single SELECT statement is allowed.")
                else:
                    try:
                        with sa_db.connect() as conn:
                            out = pd.read_sql_query(s + " LIMIT 1000", conn)
                        st.dataframe(out, use_container_width=True, height=340)
                    except Exception as e:  # noqa: BLE001
                        st.error(f"Query error: {e}")

    # ============================= Live scraper =============================
    with t_live:
        st.info("**Use case:** ad-hoc — fetch any URL through the shared proxy and extract by CSS "
                "class/selector. Same tool as domestic; useful to reverse-engineer new SA pages.")
        url = st.text_input("URL", key="sa_live_url")
        cA, cB = st.columns(2)
        if cA.button("🔍 Analyze page classes", key="sa_live_an") and url.strip():
            try:
                cl = sa_scraper._build_client({}, lambda m: None)
                html = cl.get_text(url.strip())
                info = _eng.analyze_page(html)
                st.session_state["sa_live_html"] = html
                st.caption(f"Title: {info.get('title','')}")
                st.dataframe(pd.DataFrame(info.get("classes", [])), use_container_width=True, height=260)
            except Exception as e:  # noqa: BLE001
                st.error(f"Fetch/analyze failed: {e}")
        sel = cB.text_input("CSS selector or class list (comma-sep)", key="sa_live_sel")
        mode = st.radio("Extract", ["text", "html", "href"], horizontal=True, key="sa_live_mode")
        if st.button("📤 Extract", key="sa_live_ex") and sel.strip() and st.session_state.get("sa_live_html"):
            html = st.session_state["sa_live_html"]
            try:
                if "," in sel or not any(ch in sel for ch in ".#[> "):
                    rows = _eng.extract_by_classes(html, [s.strip() for s in sel.split(",") if s.strip()], mode)
                else:
                    rows = _eng.extract_by_selector(html, sel.strip(), mode)
                st.dataframe(pd.DataFrame(rows), use_container_width=True, height=320)
            except Exception as e:  # noqa: BLE001
                st.error(f"Extract failed: {e}")

    # ============================= Reporting =============================
    with t_report:
        st.info("**Use case:** analytics — programs by country / mode / degree, tuition distribution "
                "(native + INR), top universities, and exam-requirement mix.")
        if V.counts()["programs"] == 0:
            st.caption("Run the Programs phase to populate analytics.")
        else:
            a, b = st.columns(2)
            with a:
                st.markdown("**Programs by country**")
                st.bar_chart(_agg("SELECT country_code AS k, COUNT(*) AS n FROM sa_programs "
                                  "WHERE country_code<>'' GROUP BY k ORDER BY n DESC LIMIT 30")
                             .set_index("k")["n"], height=260)
            with b:
                st.markdown("**By attendance mode**")
                st.bar_chart(_agg("SELECT COALESCE(NULLIF(program_type,''),'—') AS k, COUNT(*) AS n "
                                  "FROM sa_programs GROUP BY k ORDER BY n DESC")
                             .set_index("k")["n"], height=260)
            st.markdown("**Top degree tags**")
            st.bar_chart(_agg("SELECT COALESCE(NULLIF(course_tags,''),'—') AS k, COUNT(*) AS n "
                              "FROM sa_programs GROUP BY k ORDER BY n DESC LIMIT 20")
                         .set_index("k")["n"], height=240)
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Tuition (INR/yr) distribution**")
                fb = _agg(
                    "SELECT CASE "
                    "WHEN fee_inr_amount<=500000 THEN '0-5L' WHEN fee_inr_amount<=1000000 THEN '5-10L' "
                    "WHEN fee_inr_amount<=2000000 THEN '10-20L' WHEN fee_inr_amount<=4000000 THEN '20-40L' "
                    "WHEN fee_inr_amount<=8000000 THEN '40-80L' ELSE '80L+' END AS k, COUNT(*) AS n, "
                    "MIN(fee_inr_amount) mn FROM sa_programs WHERE fee_inr_amount>0 GROUP BY k ORDER BY mn")
                if not fb.empty:
                    st.bar_chart(fb.set_index("k")["n"], height=240)
            with c2:
                st.markdown("**Fees by currency**")
                st.bar_chart(_agg("SELECT COALESCE(NULLIF(fee_currency,''),'—') AS k, COUNT(*) AS n "
                                  "FROM sa_programs GROUP BY k ORDER BY n DESC LIMIT 15")
                             .set_index("k")["n"], height=240)
            st.markdown("**Top universities by program count**")
            st.dataframe(_agg("SELECT university_name, country_code, COUNT(*) AS programs "
                              "FROM sa_programs WHERE university_name<>'' GROUP BY university_id "
                              "ORDER BY programs DESC LIMIT 30"), use_container_width=True, height=300)
            st.markdown("**Exam requirements**")
            st.bar_chart(_agg("SELECT short_form AS k, COUNT(*) AS n FROM sa_program_exams "
                              "WHERE short_form<>'' GROUP BY k ORDER BY n DESC LIMIT 15")
                         .set_index("k")["n"], height=240)

    # ============================= Indexing =============================
    with t_index:
        st.info("**Use case:** crawl progress + coverage — which partitions are done, and how the "
                "scraped country counts compare to the site's facet counts.")
        c = V.counts()
        i1, i2, i3, i4 = st.columns(4)
        i1.metric("Programs", f"{c['programs']:,}")
        i2.metric("Partitions done", f"{c['partitions_done']:,}")
        i3.metric("Staged", f"{sa_db.staging_count():,}")
        i4.metric("Countries", f"{c['countries']:,}")
        st.markdown("**Partition progress**")
        st.dataframe(_agg("SELECT partition_key, status, last_page, found, "
                          "datetime(updated_at,'unixepoch') AS updated FROM sa_program_progress "
                          "ORDER BY updated_at DESC LIMIT 300"), use_container_width=True, height=300)
        st.markdown("**Coverage by country — facet count vs scraped**")
        cov = _agg(
            "SELECT f.label AS country, f.count AS on_site, "
            "COALESCE(p.n,0) AS scraped FROM sa_facets f "
            "LEFT JOIN (SELECT country_code, COUNT(*) n FROM sa_programs GROUP BY country_code) p "
            "ON LOWER(f.label)=LOWER(p.country_code) WHERE f.filter_name='country' "
            "ORDER BY f.count DESC")
        st.dataframe(cov, use_container_width=True, height=300)
        st.markdown("**📈 Change log — dataset size over time**")
        snaps = sa_db.get_snapshots(100)
        if snaps:
            sdf = pd.DataFrame(snaps)[["ts", "programs", "universities", "countries"]]
            sdf["ts"] = pd.to_datetime(sdf["ts"], unit="s")
            st.line_chart(sdf.set_index("ts"), height=220)
        else:
            st.caption("No snapshots yet — take one from Data & export ▸ Maintenance.")

    # ============================= Data & export =============================
    with t_data:
        st.info("**Use case:** raw table browse + spreadsheet export (SA tables only).")
        with sa_db.connect(V.db_path) as conn:
            tbls = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sa_%' ORDER BY name")]
        tbl = st.selectbox("Table (sa_ only)", tbls, key="sa_tbl")
        if tbl:
            df = _agg(f"SELECT * FROM {tbl} LIMIT 500")
            for col in ("raw_json", "description", "payload"):
                if col in df.columns:
                    df = df.drop(columns=[col])
            st.caption(f"{len(df):,} shown (max 500)")
            st.dataframe(df, use_container_width=True, height=360)
        e1, e2 = st.columns(2)
        if e1.button("🛠️ Build .xlsx (all SA tables)", key="sa_xlsx_btn"):
            with st.spinner("Building…"):
                st.session_state["sa_xlsx"] = V.export_xlsx()
        if st.session_state.get("sa_xlsx"):
            e1.download_button("⬇️ Download SA .xlsx", data=st.session_state["sa_xlsx"],
                               file_name="study_abroad_export.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        if tbl and e2.button(f"⬇️ CSV of {tbl}", key="sa_csv_btn"):
            import sa_export
            st.session_state["sa_csv"] = (tbl, sa_export.to_csv(tbl))
        if st.session_state.get("sa_csv"):
            nm, data = st.session_state["sa_csv"]
            e2.download_button(f"⬇️ {nm}.csv", data=data, file_name=f"{nm}.csv", mime="text/csv")
        if tbl and st.button(f"⬇️ JSON of {tbl}", key="sa_json_btn"):
            import sa_export
            st.session_state["sa_json"] = (tbl, sa_export.to_json(tbl))
        if st.session_state.get("sa_json"):
            nm, data = st.session_state["sa_json"]
            st.download_button(f"⬇️ {nm}.json", data=data, file_name=f"{nm}.json",
                               mime="application/json")

        st.divider()
        with st.expander("⚠️ Maintenance / reset (SA only — never touches domestic)"):
            st.caption("Discard staging, clear logs, or wipe Study Abroad data. Domestic tables "
                       "are never affected.")
            mc1, mc2, mc3 = st.columns(3)
            if mc1.button("🧹 Discard ALL staging", key="sa_disc_all"):
                n = sa_db.discard_all_staging()
                st.warning(f"Discarded {n:,} staged rows.")
            if mc2.button("🗑️ Clear all logs", key="sa_clr_logs"):
                sa_db.clear_logs()
                st.warning("Logs cleared.")
            if mc3.button("📸 Take snapshot", key="sa_snap"):
                sa_db.add_snapshot("manual")
                st.success("Snapshot saved.")
            st.markdown("**Danger zone**")
            scope = st.radio("Wipe scope", ["Data only (keep jobs/facets)", "Everything (full reset)"],
                             key="sa_wipe_scope")
            confirm = st.checkbox("Yes, wipe Study Abroad data", key="sa_wipe_ok")
            if st.button("🔥 Wipe Study Abroad", key="sa_wipe_btn", disabled=not confirm):
                out = sa_db.wipe_sa(full=scope.startswith("Everything"))
                st.error(f"Wiped: {out}")

    # ============================= Quality =============================
    with t_quality:
        st.info("**Use case:** data-quality audit — missing fees/currency/URLs, integrity, and the "
                "unpromoted staging backlog.")
        c = V.counts()
        with sa_db.connect() as conn:
            def one(q):
                try:
                    return conn.execute(q).fetchone()[0]
                except Exception:  # noqa: BLE001
                    return 0
            no_fee = one("SELECT COUNT(*) FROM sa_programs WHERE fee_native_amount IS NULL")
            no_ccy = one("SELECT COUNT(*) FROM sa_programs WHERE fee_currency IS NULL OR fee_currency=''")
            no_url = one("SELECT COUNT(*) FROM sa_programs WHERE program_url IS NULL OR program_url=''")
            no_univ = one("SELECT COUNT(*) FROM sa_programs WHERE university_id IS NULL")
        qc = st.columns(5)
        qc[0].metric("Missing fee", f"{no_fee:,}")
        qc[1].metric("Missing currency", f"{no_ccy:,}")
        qc[2].metric("Missing program URL", f"{no_url:,}")
        qc[3].metric("No university link", f"{no_univ:,}")
        qc[4].metric("Staged (unpromoted)", f"{sa_db.staging_count():,}")
        st.caption("Duplicates are structurally impossible (unique `program_id`). "
                   "'Missing fee' is usually programs the site lists without a published fee.")
        st.markdown("**Sample: programs missing a fee**")
        st.dataframe(_agg("SELECT program_id, name, university_name, country_code, program_url "
                          "FROM sa_programs WHERE fee_native_amount IS NULL LIMIT 100"),
                     use_container_width=True, height=260)

    # ============================= History =============================
    with t_hist:
        st.info("**Use case:** every Study Abroad job with status, throughput, QC score and message.")
        jobs = sa_db.list_jobs(50)
        if jobs:
            jdf = pd.DataFrame(jobs)[["id", "phase", "status", "items_written",
                                      "quality_score", "message"]]
            st.dataframe(jdf, use_container_width=True, height=420)
        else:
            st.info("No jobs yet.")
