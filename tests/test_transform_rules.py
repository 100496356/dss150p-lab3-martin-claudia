"""Unit tests for the staging and curated transformation rules (no database needed)."""
import pandas as pd

from src.transform.curated import build_curated


def test_dedupe_keeps_latest_version(staged):
    staging, _ = staged
    cust = staging['customers']
    assert cust['customer_id'].tolist().count('C1') == 1
    assert cust.loc[cust.customer_id == 'C1', 'email'].item() == 'ana.new@example.com'
    orders = staging['orders']
    assert orders.loc[orders.order_id == 'O1', 'status'].item() == 'DELIVERED'


def test_timestamps_are_parsed_as_utc(staged):
    cust = staged[0]['customers']
    ts = cust.loc[cust.customer_id == 'C2', 'updated_at'].item()
    assert str(ts.tz) == 'UTC'
    assert ts == pd.Timestamp('2025-01-01T00:00:00Z')  # 08:00 +08:00 converted to UTC


def test_email_and_city_normalization(raw_frames):
    from src.transform.staging import stage_customers
    c, _, _ = raw_frames
    only_old = c[c.updated_at == '2025-03-01T00:00:00+00:00']
    cust, _ = stage_customers(pd.concat([only_old, c[c.customer_id == 'C2']]), 'r')
    row = cust[cust.customer_id == 'C1'].iloc[0]
    assert row['email'] == 'ana.diaz@example.com'
    assert row['city'] == 'Manila'
    assert cust.loc[cust.customer_id == 'C2', 'city'].item() == 'Quezon City'
    assert pd.isna(cust.loc[cust.customer_id == 'C2', 'email'].item())


def test_product_category_is_flattened_and_bad_price_quarantined(staged):
    staging, quarantine = staged
    prod = staging['products']
    assert {'category', 'department'} <= set(prod.columns)
    assert prod.loc[prod.product_id == 'P1', 'category'].item() == 'Computers'
    assert 'P2' not in prod['product_id'].tolist()
    assert ((quarantine.business_key == 'P2') & (quarantine.reason == 'invalid_unit_price')).any()


def test_invalid_orders_are_quarantined_with_reason(staged):
    staging, quarantine = staged
    reasons = dict(zip(quarantine.business_key, quarantine.reason))
    assert reasons['O2'] == 'invalid_quantity'
    assert reasons['O3'] == 'invalid_status'
    assert not staging['orders']['order_id'].isin(['O2', 'O3']).any()
    assert set(quarantine.columns) >= {'reason', 'raw_record', 'pipeline_run_id'}


def test_curated_amounts_and_orphans(staged):
    staging, _ = staged
    cur, q = build_curated(staging, 'test_run')
    assert sorted(cur.order_id) == ['O1', 'O7']
    reasons = dict(zip(q.business_key, q.reason))
    assert reasons == {'O4': 'orphan_customer', 'O5': 'orphan_product', 'O6': 'product_quarantined_invalid_price'}
    o1 = cur[cur.order_id == 'O1'].iloc[0]
    assert o1.gross_amount == 200.00 and o1.discount_amount == 20.00 and o1.net_amount == 180.00
    assert o1.customer_city == 'Makati' and o1.category == 'Computers'
    assert set(cur.pipeline_run_id) == {'test_run'} and cur.record_hash.str.len().eq(64).all()


def test_record_hash_is_stable_across_runs(staged):
    staging, _ = staged
    a, _ = build_curated(staging, 'run_a')
    b, _ = build_curated(staging, 'run_b')
    assert a.record_hash.tolist() == b.record_hash.tolist()
    changed = {k: v.copy() for k, v in staging.items()}
    changed['orders'].loc[changed['orders'].order_id == 'O1', 'quantity'] = 3
    c, _ = build_curated(changed, 'run_c')
    assert c.loc[c.order_id == 'O1', 'record_hash'].item() != a.loc[a.order_id == 'O1', 'record_hash'].item()


def test_partition_columns(staged):
    cur, _ = build_curated(staged[0], 'r')
    assert cur.set_index('order_id').loc['O7', ['order_year', 'order_month']].tolist() == [2026, 2]
