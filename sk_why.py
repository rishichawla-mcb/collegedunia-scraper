"""
Why did a Shiksha job stop, and what is left? One command, no guessing.

    python sk_why.py          # the newest job
    python sk_why.py 7        # a specific job

A job that is no longer running stopped for exactly one of five reasons, and the
row records which. This prints the reason rather than inferring it from the
numbers, because "50,850 of 56,563" is consistent with a budget, a user stop, a
container restart and a crash alike — and those want different responses:

  completed              nothing left in the queue.
  stopped / budget       a request or bandwidth cap was hit. Raise it and resume.
  stopped / user         someone pressed Stop.
  stopped / interrupted  the worker process vanished — a container restart (a
                         deploy does this) or an OOM kill. The reaper writes
                         this only after 300s of silence AND a dead pid, so it
                         is never said about a healthy job.
  error                  the runner itself raised.

Nothing here writes. It is safe to run while a job is going.
"""
from __future__ import annotations

BUILD = "2026-09-28a"

import sys
import time
from typing import Any, Dict, Optional

import sk_db


def ago(ts: Optional[float]) -> str:
    if not ts:
        return "—"
    d = max(0.0, time.time() - float(ts))
    if d < 90:
        return "%.0fs ago" % d
    if d < 5400:
        return "%.0f min ago" % (d / 60)
    return "%.1f h ago" % (d / 3600)


def dur(secs: float) -> str:
    if secs <= 0:
        return "—"
    if secs < 5400:
        return "%.0f min" % (secs / 60)
    return "%.1f h" % (secs / 3600)


def verdict(j: Dict[str, Any]) -> str:
    """Name the cause from the row, not from the numbers."""
    st = (j.get("status") or "").lower()
    msg = (j.get("message") or "").lower()
    if st == "running":
        quiet = time.time() - float(j.get("updated_at") or 0)
        if quiet > 300:
            return ("says 'running' but has been silent for %s — the reaper "
                    "will finalise it on the next app start" % dur(quiet))
        return "still running"
    if st == "completed":
        return "finished the queue"
    if st == "error":
        return "the runner raised — see the message"
    if "worker process gone" in msg or "interrupted" in msg:
        return ("the worker process vanished: a container restart (a DEPLOY "
                "does this) or an out-of-memory kill. Nothing was lost.")
    if "budget" in msg:
        return "a budget cap was hit — raise it and resume"
    if "stopped by user" in msg:
        return "someone pressed Stop"
    if st == "stopped":
        return "stopped, and the message does not name a known cause"
    return st or "unknown"


def main(argv) -> int:
    sk_db.init_db()
    jobs = sk_db.list_jobs(50)
    if not jobs:
        print("no jobs in %s" % sk_db.SK_DB_PATH)
        return 0
    jid = int(argv[0]) if argv else jobs[0]["id"]
    j = sk_db.get_job(jid)
    if not j:
        print("no job #%s" % jid)
        return 1

    done = int(j.get("done_units") or 0)
    total = int(j.get("total_units") or 0)
    print("Shiksha job #%s · %s [BUILD %s]" % (j["id"], j.get("phase"), BUILD))
    print("  status     %s" % (j.get("status") or "—"))
    print("  WHY        %s" % verdict(j))
    print("  message    %s" % (j.get("message") or "—"))
    print("  progress   %s / %s%s" % (
        f"{done:,}", f"{total:,}",
        "  (%.1f%%)" % (100.0 * done / total) if total else ""))
    print("  rows       %s written · %s requests · %.0f MB" % (
        f"{int(j.get('items_written') or 0):,}",
        f"{int(j.get('req_count') or 0):,}",
        (j.get("bytes_count") or 0) / 1048576))
    print("  started    %s      last update %s" % (
        ago(j.get("started_at")), ago(j.get("updated_at"))))
    print("  pid %s · stop_requested %s" % (j.get("pid"), j.get("stop_requested")))

    # When did work actually STOP? Not finished_at — on a reaped job that is
    # when the reaper noticed (reading it as the end time produced a 28.5h ETA
    # on 2026-09-26 when the real figure was 5.6h). And not updated_at either:
    # the reaper finalises the row with update_job(), which stamps updated_at
    # with the reap time, so a job killed by a deploy reads as "last update 73s
    # ago" hours after it died. The last LOG line is the only timestamp nothing
    # rewrites afterwards.
    last_work = None
    with sk_db.connect() as conn:
        try:
            last_work = conn.execute(
                "SELECT MAX(ts) FROM sk_logs WHERE job_id=?", (jid,)).fetchone()[0]
        except Exception:  # noqa: BLE001
            pass
    if last_work:
        print("  last work  %s   (the newest log line; updated_at above can be "
              "the reaper's own stamp)" % ago(last_work))
    span = (float(last_work or j.get("updated_at") or 0)
            - float(j.get("started_at") or 0))
    if span > 0 and done:
        rate = done / span
        # Print per minute below 0.5/s: "0.00 colleges/s" beside a non-zero ETA
        # reads like a contradiction when the real figure is 0.06/s.
        shown = ("%.2f/s" % rate if rate >= 0.5
                 else "%.1f/min" % (rate * 60))
        print("  rate       %s over %s of work" % (shown, dur(span)))
    else:
        rate = 0.0

    # Cost per unit from the job's OWN counters rather than a constant. The
    # byte counter is WIRE bytes; the per-GET line in the crawl log prints the
    # decompressed body, which is ~5.7x larger on Shiksha. Multiplying the log
    # figure by the queue size overstates the crawl by that factor.
    kb_each = ((j.get("bytes_count") or 0) / 1024.0 / done) if done else 0.0
    if kb_each:
        print("  cost       %.0f KB wire per unit (measured by this job)"
              % kb_each)

    print("\nqueue state (the truth, independent of any job row)")
    with sk_db.connect() as conn:
        for table, label in (("sk_college_progress", "Ⓑ detail"),
                             ("sk_course_progress", "Ⓒ courses")):
            try:
                rows = list(conn.execute(
                    "SELECT status, COUNT(*) FROM %s GROUP BY status "
                    "ORDER BY 2 DESC" % table))
            except Exception:  # noqa: BLE001
                continue
            if rows:
                print("  %-10s %s" % (label, "  ".join(
                    "%s=%s" % (r[0], f"{r[1]:,}") for r in rows)))
    left_b = len(sk_db.colleges_pending())
    print("  %-10s %s colleges still pending" % ("Ⓑ left", f"{left_b:,}"))
    if rate:
        print("  %-10s ≈%s at the rate above, ≈%.1f GB" % (
            "Ⓑ eta", dur(left_b / rate), left_b * (kb_each or 171) / 1048576))
    try:
        left_c = len(sk_db.colleges_pending_courses())
        print("  %-10s %s colleges ready for phase Ⓒ" % ("Ⓒ left", f"{left_c:,}"))
    except Exception:  # noqa: BLE001
        pass

    print("\nlast log lines")
    with sk_db.connect() as conn:
        lines = [r[0] for r in conn.execute(
            "SELECT message FROM sk_logs WHERE job_id=? ORDER BY id DESC "
            "LIMIT 12", (jid,))]
    for line in reversed(lines):
        print("  %s" % str(line)[:160])
    if not lines:
        print("  (none)")

    if (j.get("status") or "") not in ("running", "completed"):
        print("\nto continue: the queue is self-draining, so simply start phase "
              "%r again — every college already marked done or gone leaves the "
              "queue and is not re-fetched." % j.get("phase"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
