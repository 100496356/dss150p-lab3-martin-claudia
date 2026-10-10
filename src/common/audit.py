"""Audit helpers: run identifiers, record hashing and run bookkeeping."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
import re
import uuid

from src.config import path_for

_SAFE = re.compile(r'[^A-Za-z0-9_.-]+')
LATEST_FILE = '_LATEST_RUN_ID'


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_run_id(run_id: str) -> str:
    """Make a run id safe to use as a folder name (Airflow ids contain ':' and '+')."""
    return _SAFE.sub('_', run_id).strip('_')


def new_run_id() -> str:
    raw = os.getenv('PIPELINE_RUN_ID') or f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"
    return safe_run_id(raw)


def remember_run_id(run_id: str) -> None:
    raw_dir = path_for('raw_dir')
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / LATEST_FILE).write_text(run_id, encoding='utf-8')


def current_run_id() -> str:
    """Run id for steps after extract: PIPELINE_RUN_ID if set, otherwise the latest extract."""
    env = os.getenv('PIPELINE_RUN_ID')
    if env:
        return safe_run_id(env)
    latest = path_for('raw_dir') / LATEST_FILE
    if not latest.exists():
        raise RuntimeError('No run id found. Run "python -m src.cli extract" first or set PIPELINE_RUN_ID.')
    return latest.read_text(encoding='utf-8').strip()


def record_hash(record: dict, keys: list[str]) -> str:
    payload = {k: record.get(k) for k in keys}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def append_event(event: dict) -> Path:
    """Append a JSON line to data/runs/events.jsonl (works even when the database is down)."""
    runs_dir = path_for('raw_dir').parent / 'runs'
    runs_dir.mkdir(parents=True, exist_ok=True)
    out = runs_dir / 'events.jsonl'
    with out.open('a', encoding='utf-8') as f:
        f.write(json.dumps({'logged_at_utc': utc_now_iso(), **event}, default=str) + '\n')
    return out
