"""Data quality checks for the curated layer."""
import pandas as pd

from src.config import SETTINGS

REQUIRED_AUDIT_FIELDS = ['pipeline_run_id', 'processed_at_utc', 'record_hash']


def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors (empty list = valid)."""
    rules = SETTINGS['quality']
    errors: list[str] = []
    if df is None or len(df) == 0:
        return ['curated dataset is empty']

    missing_cols = [c for c in ['order_id', 'quantity', 'status', 'gross_amount', 'discount_amount',
                                'net_amount', *REQUIRED_AUDIT_FIELDS] if c not in df.columns]
    if missing_cols:
        return [f'missing required columns: {missing_cols}']

    n_null = int(df['order_id'].isna().sum())
    if n_null:
        errors.append(f'{n_null} rows with NULL order_id')
    n_dup = int(df['order_id'].dropna().duplicated().sum())
    if n_dup:
        errors.append(f'{n_dup} duplicate order_id values')

    q = pd.to_numeric(df['quantity'], errors='coerce')
    n_q = int((~q.between(rules['min_quantity'], rules['max_quantity'])).sum())
    if n_q:
        errors.append(f"{n_q} rows with quantity outside [{rules['min_quantity']}, {rules['max_quantity']}]")

    for col in ['gross_amount', 'discount_amount', 'net_amount']:
        n_neg = int((pd.to_numeric(df[col], errors='coerce').fillna(-1) < 0).sum())
        if n_neg:
            errors.append(f'{n_neg} rows with negative or missing {col}')

    if (df['net_amount'] - (df['gross_amount'] - df['discount_amount'])).abs().max() > 0.01:
        errors.append('net_amount does not equal gross_amount - discount_amount')

    bad_status = sorted(set(df['status'].dropna()) - set(rules['allowed_order_statuses']))
    if bad_status or df['status'].isna().any():
        errors.append(f'statuses not allowed: {bad_status or ["NULL"]}')

    for col in REQUIRED_AUDIT_FIELDS:
        n_missing = int(df[col].isna().sum() + (df[col].astype(str).str.len() == 0).sum())
        if n_missing:
            errors.append(f'{n_missing} rows missing audit field {col}')
    return errors
