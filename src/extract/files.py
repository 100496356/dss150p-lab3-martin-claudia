from pathlib import Path
import shutil

from src.config import path_for

SOURCE_FILES = ('customers.csv', 'products.json', 'orders.csv')


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into data/raw/run_id=<run_id>/ and return that path.

    Source files are only read, never modified. Re-extracting the same run id overwrites
    that run's snapshot, so the step is safe to retry.
    """
    source_dir = path_for('source_dir')
    raw_dir = path_for('raw_dir') / f'run_id={run_id}'
    raw_dir.mkdir(parents=True, exist_ok=True)
    for name in SOURCE_FILES:
        src = source_dir / name
        if not src.exists():
            raise FileNotFoundError(f'Missing source file: {src}')
        shutil.copy2(src, raw_dir / name)
    return raw_dir
