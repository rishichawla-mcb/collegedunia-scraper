"""
Multi-vertical scraping platform — the generic framework.

This module contains ZERO vertical-specific logic. Every vertical (Domestic,
Study Abroad, MBBS Abroad, Rankings, Exams, Scholarships, ...) is described by a
`Vertical` object and registered here. The worker, UI, and exports drive verticals
through this interface only — so adding a new vertical NEVER requires changing the
framework, the worker, or the UI shell.

Data isolation: every vertical declares its OWN SQLite database file (`db_path`).
A failure or corruption in one vertical's DB cannot touch another's.

Contract a vertical must satisfy:
  - name / label          : identity
  - db_path               : its own SQLite file (isolation)
  - init_db()             : build/migrate its schema
  - phases: [Phase]       : ordered scraping phases, each with a runner
  - counts() -> dict      : headline row counts for its Overview/stats
  - export_xlsx() -> bytes: (optional) spreadsheet of its data
  - proxy_from_cfg        : (optional) how it builds an HTTP client config

Nothing here imports a specific vertical; verticals import THIS.
"""
from __future__ import annotations

BUILD = "2026-07-23a"  # platform framework build stamp

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

# A phase runner is: runner(job_id: int, cfg: dict, log: callable) -> None
Runner = Callable[[int, Dict[str, Any], Callable[[str], None]], None]


@dataclass
class Phase:
    """One scraping phase within a vertical (e.g. 'programs', 'details')."""
    id: str
    label: str
    description: str
    runner: Runner
    depends_on: List[str] = field(default_factory=list)


@dataclass
class Vertical:
    """A self-contained scraping vertical. Declares its own DB file (isolation)
    and its phases. The framework treats every vertical identically."""
    name: str                                   # machine id, e.g. 'studyabroad'
    label: str                                  # UI label, e.g. '🌍 Study Abroad'
    db_path: str                                # OWN sqlite file — data isolation
    init_db: Callable[[], None]
    phases: List[Phase]
    counts: Callable[[], Dict[str, int]]
    export_xlsx: Optional[Callable[[], bytes]] = None
    description: str = ""
    # Generic bookkeeping hooks so the worker/UI can drive any vertical without
    # knowing its internals. A vertical stores jobs/logs in its OWN db.
    get_job: Optional[Callable[[int], Dict[str, Any]]] = None       # (job_id) -> job dict
    make_logger: Optional[Callable[[int], Callable[[str], None]]] = None  # (job_id) -> log fn
    # Needed for orphan recovery: a job whose worker process dies with the
    # container leaves its row stuck at 'running' forever, and the UI then shows
    # a Stop button that can never succeed (Stop only sets a DB flag; there is no
    # process left to read it). See reap_stale_jobs().
    list_jobs: Optional[Callable[..., List[Dict[str, Any]]]] = None  # (limit) -> [job dict]
    update_job: Optional[Callable[..., None]] = None                 # (job_id, **fields)

    def phase(self, phase_id: str) -> Phase:
        for p in self.phases:
            if p.id == phase_id:
                return p
        raise KeyError(f"vertical {self.name!r} has no phase {phase_id!r}")


# ---------------------------------------------------------------------------
# Registry — verticals register themselves at import time.
# ---------------------------------------------------------------------------
_REGISTRY: Dict[str, Vertical] = {}


def register(v: Vertical) -> Vertical:
    if v.name in _REGISTRY:
        # idempotent: re-registering (e.g. re-import) replaces cleanly
        pass
    _REGISTRY[v.name] = v
    return v


def get(name: str) -> Vertical:
    if name not in _REGISTRY:
        raise KeyError(f"unknown vertical {name!r}; registered: {list(_REGISTRY)}")
    return _REGISTRY[name]


def all_verticals() -> List[Vertical]:
    return list(_REGISTRY.values())


def names() -> List[str]:
    return list(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Orphan recovery
# ---------------------------------------------------------------------------
# A worker runs as its own OS process. When the container restarts (a deploy,
# an OOM-kill, a Render suspension) the process dies but its DB row is never
# finalised: it stays 'running' forever. The UI then offers a Stop button that
# can never work, because Stop only writes stop_requested=1 and nothing is left
# alive to read it. This happened three times on 2026-09-02 across cf_jobs and
# sa_jobs. No data is ever at risk — every queue is self-draining — but the
# status is a lie, which is worse than a crash that announces itself.
REAP_GRACE_SECONDS = 300


def pid_alive(pid) -> bool:
    """True if `pid` names a live process. Signal 0 checks existence only."""
    if not pid:
        return False
    import os as _os
    try:
        _os.kill(int(pid), 0)
        return True
    except PermissionError:
        return True          # exists, just not ours to signal
    except Exception:        # noqa: BLE001  — ESRCH (gone), bad value, no os.kill
        return False


def reap_stale_jobs(v: "Vertical", grace: float = REAP_GRACE_SECONDS,
                    limit: int = 25) -> List[int]:
    """Finalise jobs whose worker is gone. Returns the ids changed.

    Status is set to 'stopped', not a new 'interrupted' value, deliberately: the
    UIs gate the Resume button on status in ('stopped','error'), so inventing a
    status would leave these rows un-resumable. The message carries the reason.

    Two guards keep this from ever touching a healthy job:
      * the row must have gone `grace` seconds without an update — a live worker
        pushes progress every 5s, so a running job is never quiet that long;
      * if the row carries a pid and that pid is alive, it is left alone.

    A NULL pid alone is NOT treated as dead: a job launched a moment ago has not
    yet recorded one. Only metadata changes; scraped rows are never touched.
    """
    if not (v.list_jobs and v.update_job):
        return []
    import time as _time
    now, changed = _time.time(), []
    try:
        jobs = v.list_jobs(limit)
    except Exception:  # noqa: BLE001
        return []
    for j in jobs or []:
        if j.get("status") not in ("running", "queued"):
            continue
        if pid_alive(j.get("pid")):
            continue
        last = j.get("updated_at") or j.get("started_at") or 0
        if last and (now - float(last)) < grace:
            continue        # too recent to call dead — let it breathe
        try:
            v.update_job(j["id"], status="stopped", finished_at=now,
                         stop_requested=0,
                         message="interrupted — worker process gone (container "
                                 "restart or crash). Nothing was lost; Resume "
                                 "continues from saved progress.")
            changed.append(j["id"])
        except Exception:  # noqa: BLE001
            pass
    return changed


def reap_all(grace: float = REAP_GRACE_SECONDS) -> Dict[str, List[int]]:
    """Reap every registered vertical. Safe to call on every app start."""
    return {v.name: reap_stale_jobs(v, grace) for v in all_verticals()}


def run_phase(vertical_name: str, phase_id: str, job_id: int,
              cfg: Dict[str, Any], log: Callable[[str], None]) -> None:
    """Generic entry point: look up the vertical + phase and run it. No
    vertical-specific branching lives here."""
    get(vertical_name).phase(phase_id).runner(job_id, cfg, log)


def run_job(vertical_name: str, job_id: int) -> None:
    """Fully generic job runner used by the platform worker: reads the vertical's
    own job (phase + config) via its `get_job` hook, builds its logger, and
    dispatches. Works for ANY registered vertical with no special-casing."""
    import json as _json
    v = get(vertical_name)
    if not v.get_job:
        raise RuntimeError(f"vertical {vertical_name!r} has no get_job hook")
    job = v.get_job(job_id)
    if not job:
        raise RuntimeError(f"{vertical_name}: job {job_id} not found")
    cfg = job.get("config_json")
    cfg = _json.loads(cfg) if isinstance(cfg, str) else (cfg or {})
    try:                       # re-inject credentials stripped at create time
        import db as _coredb
        cfg = _coredb.hydrate_secrets(cfg)
    except Exception:
        pass
    log = (v.make_logger(job_id) if v.make_logger else (lambda m: print(m, flush=True)))
    run_phase(vertical_name, job["phase"], job_id, cfg, log)
