"""Raw -> staging: typing, cleaning, deduplication and quarantine of invalid records."""
from pathlib import Path
import json

import pandas as pd

from src.common.audit import utc_now_iso
from src.config import SETTINGS

QUARANTINE_COLUMNS = ['entity', 'business_key', 'reason', 'raw_record', 'pipeline_run_id', 'quarantined_at_utc']
EMAIL_PATTERN = r'^[^@\s]+@[^@\s]+\.[^@\s]+$'


def _quarantine(df: pd.DataFrame, entity: str, key: str, reason: str, run_id: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)
    return pd.DataFrame({
        'entity': entity,
        'business_key': df[key].astype(str).values,
        'reason': reason,
        'raw_record': [json.dumps(r, default=str) for r in df.to_dict(orient='records')],
        'pipeline_run_id': run_id,
        'quarantined_at_utc': utc_now_iso(),
    })


def dedupe_latest(df: pd.DataFrame, key: str, updated_col: str = 'updated_at') -> pd.DataFrame:
    """Keep one row per business key: the version with the greatest updated_at."""
    return (df.sort_values([key, updated_col], kind='mergesort')
              .drop_duplicates(subset=[key], keep='last')
              .reset_index(drop=True))


def to_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors='coerce')


def stage_customers(raw: pd.DataFrame, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = raw.copy()
    df['created_at'] = to_utc(df['created_at'])
    df['updated_at'] = to_utc(df['updated_at'])
    df = dedupe_latest(df, 'customer_id')
    # Normalize: emails lower-case/trimmed (missing stays NULL), cities trimmed and title-cased.
    df['email'] = df['email'].astype('string').str.strip().str.lower().replace('', pd.NA)
    df['email_valid'] = df['email'].fillna('').str.match(EMAIL_PATTERN)
    df['city'] = df['city'].astype('string').str.strip().str.title()
    df['customer_tier'] = df['customer_tier'].astype('string').str.strip().str.title()
    bad = df[df['customer_id'].isna() | df['updated_at'].isna()]
    good = df.drop(bad.index)
    return good.reset_index(drop=True), _quarantine(bad, 'customer', 'customer_id', 'missing_key_or_timestamp', run_id)


def stage_products(raw_records: list[dict], run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.json_normalize(raw_records)
    df = df.rename(columns={'name': 'product_name', 'category.name': 'category', 'category.department': 'department'})
    df['updated_at'] = to_utc(df['updated_at'])
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df = dedupe_latest(df, 'product_id')
    bad_price = df[df['unit_price'].isna() | (df['unit_price'] <= 0)]
    good = df.drop(bad_price.index)
    q = _quarantine(bad_price, 'product', 'product_id', 'invalid_unit_price', run_id)
    good = good.reset_index(drop=True)
    # Remember quarantined ids so curated can explain why their orders are excluded.
    good.attrs['quarantined_ids'] = sorted(bad_price['product_id'].astype(str))
    return good, q


def stage_orders(raw: pd.DataFrame, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    rules = SETTINGS['quality']
    df = raw.copy()
    df['order_timestamp'] = to_utc(df['order_timestamp'])
    df['updated_at'] = to_utc(df['updated_at'])
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['discount_pct'] = pd.to_numeric(df['discount_pct'], errors='coerce')
    df['status'] = df['status'].astype('string').str.strip().str.upper()
    df = dedupe_latest(df, 'order_id')

    checks = [
        ('missing_timestamp', df['order_timestamp'].isna() | df['updated_at'].isna()),
        ('invalid_quantity', ~df['quantity'].between(rules['min_quantity'], rules['max_quantity'])),
        ('invalid_status', ~df['status'].isin(rules['allowed_order_statuses'])),
        ('invalid_unit_price', df['unit_price'].isna() | (df['unit_price'] <= 0)),
        ('invalid_discount_pct', ~df['discount_pct'].between(0, 1)),
    ]
    quarantined, bad_idx = [], set()
    for reason, mask in checks:
        rows = df[mask.fillna(True) & ~df.index.isin(bad_idx)]
        if not rows.empty:
            quarantined.append(_quarantine(rows, 'order', 'order_id', reason, run_id))
            bad_idx.update(rows.index)
    good = df.drop(index=list(bad_idx)).copy()
    good['quantity'] = good['quantity'].astype(int)
    q = pd.concat(quarantined, ignore_index=True) if quarantined else pd.DataFrame(columns=QUARANTINE_COLUMNS)
    return good.reset_index(drop=True), q


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets.

    Returns ({'customers', 'products', 'orders'} -> DataFrame, quarantine DataFrame).
    Every staging row gets pipeline_run_id and staged_at_utc audit columns.
    """
    raw_dir = Path(raw_dir)
    customers, q_c = stage_customers(pd.read_csv(raw_dir / 'customers.csv', dtype=str), run_id)
    with (raw_dir / 'products.json').open(encoding='utf-8') as f:
        products, q_p = stage_products(json.load(f), run_id)
    orders, q_o = stage_orders(pd.read_csv(raw_dir / 'orders.csv', dtype=str), run_id)

    staged_at = pd.Timestamp.now(tz='UTC')
    staging = {'customers': customers, 'products': products, 'orders': orders}
    for df in staging.values():
        df['pipeline_run_id'] = run_id
        df['staged_at_utc'] = staged_at
    quarantine = pd.concat([q_c, q_p, q_o], ignore_index=True)
    return staging, quarantine
