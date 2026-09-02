"""
Generic platform worker — drives ANY registered vertical.

    python platform_worker.py <vertical> <job_id>

It imports the vertical registrations, initialises that vertical's (isolated) DB,
and runs the job through the framework. No vertical-specific code lives here, so
new verticals need zero changes to the worker.
"""
from __future__ import annotations

BUILD = "2026-07-23a"

import os
import sys

import vertical_base as vb

# Import each vertical module so it self-registers. Add future verticals here.
import sa_vertical  # noqa: F401  (registers 'studyabroad')
import cf_vertical  # noqa: F401  (registers 'coursefinder')
_VERTICALS = (sa_vertical, cf_vertical)   # referenced so pyflakes keeps the self-registration imports


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: python platform_worker.py <vertical> <job_id>", flush=True)
        sys.exit(1)
    vertical, job_id = sys.argv[1], int(sys.argv[2])
    v = vb.get(vertical)
    v.init_db()
    # Record our pid so orphan recovery can tell a live run from a dead one.
    # worker.py (domestic) has always done this; the vertical worker did not,
    # which is exactly why cf_jobs/sa_jobs accumulated rows stuck at 'running'
    # after container restarts. Best-effort: never block the run on bookkeeping.
    if v.update_job:
        try:
            v.update_job(job_id, pid=os.getpid())
        except Exception:  # noqa: BLE001
            pass
    vb.run_job(vertical, job_id)


if __name__ == "__main__":
    main()
