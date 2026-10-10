"""Integration tests for rerun-safe loading. Skipped automatically if PostgreSQL is not reachable.

They use order ids starting with TEST_ and delete them afterwards, so real data is untouched.
"""
import pandas as pd
import pytest

from src.transform.curated import build_curated

psycopg = pytest.importorskip('psycopg')


def _db_available() -> bool:
    try:
        from src.common.db import connect
        with connect() as conn:
            conn.execute('SELECT 1 FROM curated.sales_order_lines LIMIT 1')
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _db_available(), reason='PostgreSQL not reachable (start docker compose postgres)')


@pytest.fixture
def test_rows(staged):
    from src.common.db import connect
    cur, _ = build_curated(staged[0], 'pytest_run')
    cur['order_id'] = 'TEST_' + cur['order_id']
    yield cur
    with connect() as conn:
        conn.execute("DELETE FROM curated.sales_order_lines WHERE order_id LIKE 'TEST\\_%'")
        conn.execute("DELETE FROM audit.partition_loads WHERE pipeline_run_id LIKE 'pytest%'")


def _count_test_rows():
    from src.common.db import connect
    with connect() as conn:
        return conn.execute("SELECT count(*) FROM curated.sales_order_lines WHERE order_id LIKE 'TEST\\_%'").fetchone()[0]


def test_rerun_does_not_duplicate_or_update(test_rows):
    from src.load.postgres import upsert_curated
    assert upsert_curated(test_rows, 'pytest_run_1') == len(test_rows)
    assert upsert_curated(test_rows, 'pytest_run_2') == 0  # same content -> nothing affected
    assert _count_test_rows() == len(test_rows)


def test_changed_record_is_updated_not_duplicated(test_rows):
    from src.common.db import connect
    from src.load.postgres import upsert_curated
    upsert_curated(test_rows, 'pytest_run_1')
    changed = test_rows.copy()
    changed.loc[0, 'status'] = 'CANCELLED'
    changed.loc[0, 'record_hash'] = 'f' * 64
    assert upsert_curated(changed, 'pytest_run_2') == 1
    assert _count_test_rows() == len(test_rows)
    with connect() as conn:
        status = conn.execute('SELECT status FROM curated.sales_order_lines WHERE order_id = %s',
                              (changed.loc[0, 'order_id'],)).fetchone()[0]
    assert status == 'CANCELLED'


def test_load_partition_only_loads_selected_month(test_rows):
    from src.common.db import connect
    from src.load.postgres import load_partition
    assert sorted(test_rows.order_month) == [1, 2]
    rows = test_rows.assign(order_year=2099)  # fake year so the real 2026 audit record is never touched
    affected = load_partition(rows, 2099, 2, 'pytest_part')
    assert affected == 1 and _count_test_rows() == 1
    with connect() as conn:
        row = conn.execute("SELECT row_count, pipeline_run_id FROM audit.partition_loads "
                           "WHERE partition_key = 'order_year=2099/order_month=02'").fetchone()
    assert row == (1, 'pytest_part')


def test_load_partition_rejects_bad_month(test_rows):
    from src.load.postgres import load_partition
    with pytest.raises(ValueError):
        load_partition(test_rows, 2026, 13, 'pytest_part')
