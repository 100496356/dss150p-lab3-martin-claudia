def validate_curated(df) -> list[str]:
    """Return a list of human-readable validation errors.

    Minimum checks: order_id uniqueness/non-null, quantity range,
    nonnegative amounts, allowed statuses, required audit fields.
    """
    from src.config import SETTINGS
    quality = SETTINGS['quality']
    errors = []

    if df.empty:
        errors.append('curated dataset is empty')
        return errors

    # order_id: non-null and unique (this is the PostgreSQL conflict key —
    # a violation here would break the whole UPSERT contract).
    null_ids = df['order_id'].isna().sum()
    if null_ids:
        errors.append(f'order_id has {null_ids} null value(s)')
    dup_ids = df['order_id'].duplicated().sum()
    if dup_ids:
        errors.append(f'order_id has {dup_ids} duplicate value(s)')

    # quantity range (same bounds as staging, re-checked here as a
    # contract assertion on the FINAL curated output, independent of
    # whether staging's own filtering logic is trusted).
    bad_qty = ((df['quantity'] < quality['min_quantity']) | (df['quantity'] > quality['max_quantity'])).sum()
    if bad_qty:
        errors.append(f'quantity out of allowed range for {bad_qty} row(s)')

    # amounts must be non-negative
    for col in ('gross_amount', 'discount_amount', 'net_amount'):
        negative = (df[col] < 0).sum()
        if negative:
            errors.append(f'{col} is negative for {negative} row(s)')

    # status must be one of the allowed values
    bad_status = (~df['status'].isin(quality['allowed_order_statuses'])).sum()
    if bad_status:
        errors.append(f'status is not an allowed value for {bad_status} row(s)')

    # required audit fields must be present and non-null
    for col in ('pipeline_run_id', 'processed_at_utc', 'record_hash'):
        missing = df[col].isna().sum()
        if missing:
            errors.append(f'{col} is null for {missing} row(s)')

    return errors
