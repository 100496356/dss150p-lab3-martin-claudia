"""Tests for the curated data-quality checks."""
import pandas as pd

from src.transform.curated import build_curated
from src.validate.quality import validate_curated


def _clean(staged):
    cur, _ = build_curated(staged[0], 'r')
    return cur


def test_clean_curated_passes(staged):
    assert validate_curated(_clean(staged)) == []


def test_detects_duplicate_and_null_order_id(staged):
    cur = _clean(staged)
    bad = pd.concat([cur, cur.iloc[[0]]], ignore_index=True)
    bad.loc[1, 'order_id'] = None
    errors = ' | '.join(validate_curated(bad))
    assert 'duplicate order_id' in errors and 'NULL order_id' in errors


def test_detects_quantity_status_and_negative_amounts(staged):
    cur = _clean(staged)
    cur.loc[0, 'quantity'] = 99
    cur.loc[0, 'status'] = 'LOST'
    cur.loc[1, 'net_amount'] = -5
    errors = ' | '.join(validate_curated(cur))
    assert 'quantity outside' in errors
    assert 'LOST' in errors
    assert 'negative or missing net_amount' in errors


def test_detects_missing_audit_fields(staged):
    cur = _clean(staged)
    cur.loc[0, 'pipeline_run_id'] = None
    assert any('pipeline_run_id' in e for e in validate_curated(cur))
    assert validate_curated(cur.drop(columns=['record_hash']))[0].startswith('missing required columns')


def test_empty_dataset_fails():
    assert validate_curated(pd.DataFrame()) == ['curated dataset is empty']
