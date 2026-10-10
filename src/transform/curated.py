"""Staging -> curated: analysis-ready sales order lines."""
import pandas as pd

from src.common.audit import record_hash
from src.transform.staging import _quarantine, QUARANTINE_COLUMNS

# Business columns that define the content of a curated row. The hash ignores audit
# columns (run id, processed time), so an unchanged record keeps the same hash on reruns.
HASH_COLUMNS = [
    'order_id', 'customer_id', 'product_id', 'order_timestamp', 'customer_city', 'customer_tier',
    'product_name', 'category', 'brand', 'quantity', 'unit_price', 'discount_pct',
    'gross_amount', 'discount_amount', 'net_amount', 'status', 'source_updated_at',
]

CURATED_COLUMNS = HASH_COLUMNS + ['pipeline_run_id', 'processed_at_utc', 'record_hash']


def compute_amounts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['gross_amount'] = (df['quantity'] * df['unit_price']).round(2)
    df['discount_amount'] = (df['gross_amount'] * df['discount_pct']).round(2)
    df['net_amount'] = (df['gross_amount'] - df['discount_amount']).round(2)
    return df


def build_curated(staging: dict, run_id: str):
    """Join staging orders/customers/products into curated.sales_order_lines rows.

    Orphan customer/product references are quarantined (reason orphan_customer /
    orphan_product), never silently dropped. Returns (curated DataFrame, quarantine DataFrame).
    """
    orders = staging['orders']
    customers = staging['customers'][['customer_id', 'city', 'customer_tier']].rename(
        columns={'city': 'customer_city'})
    products = staging['products'][['product_id', 'product_name', 'category', 'brand']]

    merged = (orders.merge(customers, on='customer_id', how='left', indicator='_cust')
                    .merge(products, on='product_id', how='left', indicator='_prod'))
    bad_products = set(staging['products'].attrs.get('quarantined_ids', []))
    orphan_c = merged['_cust'] == 'left_only'
    missing_p = (merged['_prod'] == 'left_only') & ~orphan_c
    # A product that exists in the source but was quarantined (e.g. negative price) is
    # reported separately from a product id that does not exist at all.
    bad_p = missing_p & merged['product_id'].isin(bad_products)
    orphan_p = missing_p & ~bad_p
    def q_for(mask, reason):
        return _quarantine(orders[orders['order_id'].isin(merged.loc[mask, 'order_id'])], 'order', 'order_id', reason, run_id)
    q = [q_for(orphan_c, 'orphan_customer'), q_for(orphan_p, 'orphan_product'),
         q_for(bad_p, 'product_quarantined_invalid_price')]
    quarantine = pd.concat([x for x in q if not x.empty], ignore_index=True) if any(not x.empty for x in q) \
        else pd.DataFrame(columns=QUARANTINE_COLUMNS)

    cur = merged[~(orphan_c | missing_p)].drop(columns=['_cust', '_prod'])
    cur = cur.rename(columns={'updated_at': 'source_updated_at'})
    cur = compute_amounts(cur)
    cur['pipeline_run_id'] = run_id
    cur['processed_at_utc'] = pd.Timestamp.now(tz='UTC')
    cur['record_hash'] = [record_hash(r, HASH_COLUMNS) for r in cur[HASH_COLUMNS].to_dict(orient='records')]
    cur = cur[CURATED_COLUMNS].sort_values('order_id').reset_index(drop=True)
    # Partition helpers (kept in files, not in the PostgreSQL table).
    cur['order_year'] = cur['order_timestamp'].dt.year.astype(int)
    cur['order_month'] = cur['order_timestamp'].dt.month.astype(int)
    return cur, quarantine
