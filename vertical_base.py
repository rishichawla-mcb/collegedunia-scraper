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
