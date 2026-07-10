"""
Worker entry point. Runs a single job (already created in the DB) to completion.

The Streamlit UI launches this as a detached subprocess so the scrape keeps
running across UI reruns and even if the browser tab is closed (as long as the
host process / container stays alive).

It can also be used directly from the command line:

    # create + run a courses job
    python worker.py --new courses --delay 1
    # create + run a phase-2 job limited to the first 50 courses by college count
    python worker.py --new offerings --limit-courses 50
    # run an existing job by id
    python worker.py --job 7
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import db
import scraper


def _log_factory(job_id: int):
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, f"job_{job_id}.log")

    counter = {"n": 0}

    def log(msg: str) -> None:
        line = f"[{time.strftime('%H:%M:%S')}] {msg}"
        print(line, flush=True)
        try:
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError:
            pass
        # Persist to DB (survives restarts; shown live in the UI).
        try:
            db.add_log(job_id, line)
            counter["n"] += 1
            if counter["n"] % 250 == 0:
                db.prune_logs()
        except Exception:
            pass

    return log


def run_job(job_id: int) -> None:
    db.init_db()
    job = db.get_job(job_id)
    if not job:
        print(f"No such job: {job_id}", file=sys.stderr)
        sys.exit(1)
    cfg = json.loads(job["config_json"] or "{}")
    db.update_job(job_id, pid=os.getpid())
    log = _log_factory(job_id)
    log(f"Worker PID {os.getpid()} starting job {job_id} ({job['type']})")
    if job["type"] == "courses":
        scraper.run_courses(job_id, cfg, log=log)
    elif job["type"] == "offerings":
        scraper.run_offerings(job_id, cfg, log=log)
    elif job["type"] == "pipeline":
        scraper.run_pipeline(job_id, cfg, log=log)
    elif job["type"] == "enrichment":
        scraper.run_enrichment(job_id, cfg, log=log)
    elif job["type"] == "college_courses":
        scraper.run_college_courses(job_id, cfg, log=log)
    elif job["type"] == "directory":
        scraper.run_directory(job_id, cfg, log=log)
    else:
        db.update_job(job_id, status="error", message=f"unknown job type {job['type']}")
        log(f"Unknown job type: {job['type']}")
        return

    # Governance gate: validate the job's staged data and auto-promote (or hold
    # for manual approval). Enrichment writes master directly, so it's exempt.
    if job["type"] != "enrichment" and cfg.get("staging", True):
        fin = db.get_job(job_id)
        if fin and fin["status"] == "completed" and not fin.get("promote_status"):
            scraper._finalize_job(job_id, cfg, log,
                                  base_msg=(fin.get("message") or "scrape complete"),
                                  db_path=db.DB_PATH)


def main() -> None:
    ap = argparse.ArgumentParser(description="Collegedunia scrape worker")
    ap.add_argument("--job", type=int, help="Run an existing job id")
    ap.add_argument("--new", choices=["courses", "offerings"], help="Create and run a new job")
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--max-pages", type=int, default=None, help="(courses) limit pages")
    ap.add_argument("--limit-courses", type=int, default=None,
                    help="(offerings) only the top-N courses by college count")
    ap.add_argument("--max-pages-per-course", type=int, default=None,
                    help="(offerings) cap pages per course (for testing)")
    ap.add_argument("--proxy-mode", choices=["none", "list", "gateway"], default="none")
    ap.add_argument("--proxy-gateway", default=None)
    ap.add_argument("--proxy-file", default=None, help="File with one proxy URL per line")
    args = ap.parse_args()

    db.init_db()

    if args.job:
        run_job(args.job)
        return

    if not args.new:
        ap.error("Pass --job <id> or --new courses|offerings")

    proxy_list = []
    if args.proxy_file and os.path.exists(args.proxy_file):
        with open(args.proxy_file, encoding="utf-8") as fh:
            proxy_list = [ln.strip() for ln in fh if ln.strip()]

    cfg = {
        "delay": args.delay,
        "proxy_mode": args.proxy_mode,
        "proxy_gateway": args.proxy_gateway,
        "proxy_list": proxy_list,
    }
    if args.new == "courses":
        cfg["max_pages"] = args.max_pages
    else:
        cfg["max_pages_per_course"] = args.max_pages_per_course
        if args.limit_courses:
            ids = db.list_course_ids()[: args.limit_courses]
            cfg["course_ids"] = ids

    job_id = db.create_job(args.new, cfg)
    print(f"Created job {job_id}")
    run_job(job_id)


if __name__ == "__main__":
    main()
