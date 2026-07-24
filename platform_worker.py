"""
Generic platform worker — drives ANY registered vertical.

    python platform_worker.py <vertical> <job_id>

It imports the vertical registrations, initialises that vertical's (isolated) DB,
and runs the job through the framework. No vertical-specific code lives here, so
new verticals need zero changes to the worker.
"""
from __future__ import annotations

BUILD = "2026-07-23a"

import sys

import vertical_base as vb

# Import each vertical module so it self-registers. Add future verticals here.
import sa_vertical  # noqa: F401  (registers 'studyabroad')


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: python platform_worker.py <vertical> <job_id>", flush=True)
        sys.exit(1)
    vertical, job_id = sys.argv[1], int(sys.argv[2])
    v = vb.get(vertical)
    v.init_db()
    vb.run_job(vertical, job_id)


if __name__ == "__main__":
    main()
