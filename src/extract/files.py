from pathlib import Path
import shutil
from src.config import path_for


def extract_sources(run_id: str) -> Path:
    """Copy immutable source snapshots into a run-specific raw directory.

    1. Create data/raw/run_id=<run_id>/.
    2. Copy customers.csv, products.json, and orders.csv from data/source/.
    3. Return the run-specific raw path.
    4. Do not modify source files in place.
    """
    source_dir = path_for('source_dir')
    raw_root = path_for('raw_dir')
    run_dir = raw_root / f'run_id={run_id}'
    run_dir.mkdir(parents=True, exist_ok=True)

    for filename in ('customers.csv', 'products.json', 'orders.csv'):
        src_file = source_dir / filename
        if not src_file.exists():
            raise FileNotFoundError(f'Expected source file not found: {src_file}')
        # shutil.copy2 preserves file metadata and never touches the source content.
        shutil.copy2(src_file, run_dir / filename)

    return run_dir
