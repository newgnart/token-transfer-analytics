{% snapshot snap_stablecoin %}

{{
    config(
        target_schema='snapshots',
        unique_key='contract_address || chain',
        strategy='check',
        check_cols=['symbol', 'name', 'currency', 'backing_type', 'decimals'],
        invalidate_hard_deletes=True,
        updated_at='date_trunc(\'second\', current_timestamp' ~ (' at time zone \'utc\'' if target.type == 'postgres' else '::timestamp_ntz') ~ ')'
    )
}}

    select
        contract_address,
        chain,
        symbol,
        name,
        currency,
        backing_type,
        decimals
    from {{ ref('stg_stablecoin') }}

{% endsnapshot %}
