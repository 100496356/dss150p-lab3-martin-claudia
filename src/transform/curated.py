import pandas as pd
from src.common.audit import utc_now_iso, record_hash as _record_hash

# The business fields that define a curated row's content. record_hash is
# computed only from these — never from processed_at_utc or pipeline_run_id —
# so it stays stable across reruns unless the actual business data changes.
_HASH_KEYS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp',
    'quantity', 'unit_price', 'discount_pct', 'status',
]


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products and create analysis-ready sales rows.

    Orphan customer/product references are NOT silently dropped: they are
    returned as quarantine rows with an explicit reason.

    Returns (curated_df, quarantine_df).
    """
    orders = staging['orders']
    customers = staging['customers']

    customers_small = customers[['customer_id', 'city', 'customer_tier', 'email']].rename(
        columns={'city': 'customer_city'}
    )
    # Join against the FULL product reference (existence-only), not the
    # price-quality-filtered 'products' set — a product with a bad catalog
    # price still exists, and its order lines must not be treated as orphans
    # just because of an unrelated data-quality issue on the product side.
    products_reference = staging['products_reference']
    products_small = products_reference[['product_id', 'name', 'category', 'brand']].rename(
        columns={'name': 'product_name'}
    )

    # Left-join on purpose (not inner join): an order whose customer_id or
    # product_id has no match must still appear here so we can detect and
    # quarantine it explicitly, rather than have it silently vanish the way
    # an inner join would make it vanish.
    merged = orders.merge(customers_small, on='customer_id', how='left')
    merged = merged.merge(products_small, on='product_id', how='left')

    orphan_customer = merged['customer_city'].isna() & merged['customer_tier'].isna()
    orphan_product = merged['product_name'].isna()
    orphan_mask = orphan_customer | orphan_product

    def _orphan_reason(row):
        reasons = []
        if pd.isna(row['customer_city']) and pd.isna(row['customer_tier']):
            reasons.append('orphan_customer_reference')
        if pd.isna(row['product_name']):
            reasons.append('orphan_product_reference')
        return ';'.join(reasons)

    quarantine = merged[orphan_mask].copy()
    if not quarantine.empty:
        quarantine['quarantine_reason'] = quarantine.apply(_orphan_reason, axis=1)
        quarantine['source_dataset'] = 'curated_join'

    valid = merged[~orphan_mask].copy()

    # Business calculations (unit_price is the order's own transactional
    # price at the time of sale, not the product's current catalog price).
    valid['gross_amount'] = valid['quantity'] * valid['unit_price']
    valid['discount_amount'] = valid['gross_amount'] * valid['discount_pct']
    valid['net_amount'] = valid['gross_amount'] - valid['discount_amount']

    processed_at = utc_now_iso()
    valid['source_updated_at'] = valid['updated_at']
    valid['pipeline_run_id'] = run_id
    valid['processed_at_utc'] = processed_at

    valid['record_hash'] = valid.apply(
        lambda row: _record_hash(row.to_dict(), _HASH_KEYS), axis=1
    )

    curated_columns = [
        'order_id', 'customer_id', 'product_id', 'order_timestamp',
        'customer_city', 'customer_tier', 'product_name', 'category', 'brand',
        'quantity', 'unit_price', 'discount_pct',
        'gross_amount', 'discount_amount', 'net_amount', 'status',
        'source_updated_at', 'pipeline_run_id', 'processed_at_utc', 'record_hash',
    ]
    curated = valid[curated_columns].reset_index(drop=True)

    return curated, quarantine
