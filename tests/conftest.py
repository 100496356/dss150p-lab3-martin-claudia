import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def raw_frames():
    """Small hand-made raw data with every kind of problem found in the real source files."""
    customers = pd.DataFrame([
        # duplicate customer: the newer version (C1, 2025-03-03) must win
        dict(customer_id='C1', first_name='Ana', last_name='Diaz', email='  ANA.Diaz@Example.com ', city='  manila  ',
             customer_tier='gold', created_at='2023-01-01T00:00:00+00:00', updated_at='2025-03-01T00:00:00+00:00'),
        dict(customer_id='C1', first_name='Ana', last_name='Diaz', email='ana.new@example.com', city='Makati',
             customer_tier='Gold', created_at='2023-01-01T00:00:00+00:00', updated_at='2025-03-03T00:00:00+00:00'),
        dict(customer_id='C2', first_name='Ben', last_name='Cruz', email=None, city='  quezon city ',
             customer_tier='Silver', created_at='2023-01-01T00:00:00+00:00', updated_at='2025-01-01T08:00:00+08:00'),
    ]).astype('string')
    products = [
        {'product_id': 'P1', 'name': 'Laptop', 'category': {'name': 'Computers', 'department': 'Computing'},
         'brand': 'Helix', 'unit_price': 1000.0, 'active': True, 'updated_at': '2025-01-01T00:00:00+00:00'},
        {'product_id': 'P2', 'name': 'Broken', 'category': {'name': 'Audio', 'department': 'Electronics'},
         'brand': 'Nova', 'unit_price': -199.0, 'active': True, 'updated_at': '2025-01-01T00:00:00+00:00'},
    ]
    o = dict(customer_id='C1', product_id='P1', order_timestamp='2026-01-15T10:00:00+00:00', quantity='2',
             unit_price='100.00', discount_pct='0.1', status='DELIVERED', updated_at='2026-01-15T12:00:00+00:00')
    orders = pd.DataFrame([
        {**o, 'order_id': 'O1', 'status': 'PENDING'},
        {**o, 'order_id': 'O1', 'updated_at': '2026-01-16T12:00:00+00:00'},  # newer version of O1
        {**o, 'order_id': 'O2', 'quantity': '0'},                             # invalid quantity
        {**o, 'order_id': 'O3', 'status': 'UNKNOWN'},                         # invalid status
        {**o, 'order_id': 'O4', 'customer_id': 'C999'},                       # orphan customer
        {**o, 'order_id': 'O5', 'product_id': 'P999'},                        # orphan product
        {**o, 'order_id': 'O6', 'product_id': 'P2'},                          # product quarantined
        {**o, 'order_id': 'O7', 'customer_id': 'C2', 'order_timestamp': '2026-02-01T00:00:00+00:00'},
    ]).astype('string')
    return customers, products, orders


@pytest.fixture
def staged(raw_frames):
    from src.transform.staging import stage_customers, stage_products, stage_orders
    c, p, o = raw_frames
    cust, q1 = stage_customers(c, 'test_run')
    prod, q2 = stage_products(p, 'test_run')
    orders, q3 = stage_orders(o, 'test_run')
    return {'customers': cust, 'products': prod, 'orders': orders}, pd.concat([q1, q2, q3], ignore_index=True)
