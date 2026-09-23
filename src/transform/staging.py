import json
import pandas as pd
from src.config import SETTINGS
from src.common.audit import utc_now_iso


def _dedupe_keep_latest(df: pd.DataFrame, key: str, updated_col: str = 'updated_at') -> pd.DataFrame:
    """Keep exactly one row per business key: the one with the greatest updated_at.
    Deterministic and does not depend on knowing which keys are duplicated in advance."""
    return (
        df.sort_values(updated_col)
        .drop_duplicates(subset=key, keep='last')
        .reset_index(drop=True)
    )


def _stage_customers(raw_dir, run_id: str, staged_at: str):
    df = pd.read_csv(raw_dir / 'customers.csv')

    # Parse timestamps as UTC. errors='coerce' turns any unparseable value into NaT
    # instead of crashing the whole pipeline on one bad row.
    df['created_at'] = pd.to_datetime(df['created_at'], utc=True, errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')

    # Keep the most recent version of each customer_id (handles the C00120-style
    # duplicate-version rows found during profiling).
    df = _dedupe_keep_latest(df, 'customer_id')

    # Normalize email: lowercase + trim. A missing email is left as null rather
    # than filled in or dropped, so it stays a visible, queryable quality
    # condition downstream instead of being silently hidden.
    df['email'] = df['email'].astype('string').str.strip().str.lower()

    # Normalize city: trim + title-case.
    df['city'] = df['city'].astype('string').str.strip().str.title()

    df['pipeline_run_id'] = run_id
    df['staged_at_utc'] = staged_at
    return df


def _stage_products(raw_dir, run_id: str, staged_at: str):
    records = json.loads((raw_dir / 'products.json').read_text(encoding='utf-8'))
    df = pd.json_normalize(records)
    # json_normalize flattens category.name / category.department automatically;
    # rename to the flat column names the rest of the pipeline expects.
    df = df.rename(columns={'category.name': 'category', 'category.department': 'department'})

    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')

    # Reference set: EVERY known product_id (deduplicated, keeping the latest
    # updated_at version), regardless of price validity. This is what curated
    # joins against to decide whether an order references a real product at
    # all. A product with a bad price still legitimately exists — its own
    # descriptive attributes (name/category/brand) are not corrupted, only
    # its catalog price is — so orders for it must NOT be treated as orphans.
    reference = _dedupe_keep_latest(df, 'product_id')

    # Quality-filtered set: only price-valid products, used for anything that
    # actually needs to trust unit_price (e.g. a product catalog report).
    invalid_price = df['unit_price'].isna() | (df['unit_price'] < 0)
    quarantine = df[invalid_price].copy()
    quarantine['quarantine_reason'] = 'invalid_or_negative_unit_price'
    quarantine['source_dataset'] = 'products'

    valid = df[~invalid_price].copy()
    valid = _dedupe_keep_latest(valid, 'product_id')

    for out in (reference, valid):
        out['pipeline_run_id'] = run_id
        out['staged_at_utc'] = staged_at

    return valid, reference, quarantine


def _stage_orders(raw_dir, run_id: str, staged_at: str):
    df = pd.read_csv(raw_dir / 'orders.csv')

    df['order_timestamp'] = pd.to_datetime(df['order_timestamp'], utc=True, errors='coerce')
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

    quality = SETTINGS['quality']
    allowed_statuses = set(quality['allowed_order_statuses'])

    bad_quantity = df['quantity'].isna() | (df['quantity'] < quality['min_quantity']) | (df['quantity'] > quality['max_quantity'])
    bad_status = ~df['status'].isin(allowed_statuses)
    bad_timestamp = df['order_timestamp'].isna() | df['updated_at'].isna()

    invalid_mask = bad_quantity | bad_status | bad_timestamp

    quarantine = df[invalid_mask].copy()

    def _reason(row):
        reasons = []
        if pd.isna(row['quantity']) or row['quantity'] < quality['min_quantity'] or row['quantity'] > quality['max_quantity']:
            reasons.append('quantity_out_of_range')
        if row['status'] not in allowed_statuses:
            reasons.append('status_not_allowed')
        if pd.isna(row['order_timestamp']) or pd.isna(row['updated_at']):
            reasons.append('unparseable_timestamp')
        return ';'.join(reasons)

    if not quarantine.empty:
        quarantine['quarantine_reason'] = quarantine.apply(_reason, axis=1)
        quarantine['source_dataset'] = 'orders'

    valid = df[~invalid_mask].copy()
    # Duplicate business keys (same order_id delivered again with a later
    # updated_at) are resolved only among technically-valid rows.
    valid = _dedupe_keep_latest(valid, 'order_id')

    valid['pipeline_run_id'] = run_id
    valid['staged_at_utc'] = staged_at
    return valid, quarantine


def build_staging(raw_dir, run_id: str):
    """Create cleaned, typed staging datasets.

    Returns a dict of staging DataFrames (keys: 'customers', 'products', 'orders')
    and a single combined quarantine DataFrame with a reason per rejected row.
    """
    staged_at = utc_now_iso()

    customers = _stage_customers(raw_dir, run_id, staged_at)
    products, products_reference, products_quarantine = _stage_products(raw_dir, run_id, staged_at)
    orders, orders_quarantine = _stage_orders(raw_dir, run_id, staged_at)

    quarantine_parts = [q for q in (products_quarantine, orders_quarantine) if not q.empty]
    quarantine = pd.concat(quarantine_parts, ignore_index=True) if quarantine_parts else pd.DataFrame()

    staging = {
        'customers': customers,
        'products': products,
        # Full product reference (existence-only, price not required to be
        # valid) — used by curated joins to distinguish "product doesn't
        # exist" from "product exists but had a data-quality issue".
        'products_reference': products_reference,
        'orders': orders,
    }
    return staging, quarantine
