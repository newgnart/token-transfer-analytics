{{
    config(
        materialized='incremental',
        unique_key=['transaction_hash', 'log_index'],
        on_schema_change='fail',
        incremental_strategy='delete+insert'
    )
}}

-- Fact table for Burn events (Transfer events where to_address = 0x0)
-- Built on top of int_all_transfer intermediate model


with int_all_transfer as (
    select * from {{ ref('int_all_transfer') }}
    {% if is_incremental() %}
    -- Only process new blocks since last run
        where block_number > (select COALESCE(MAX(block_number), 0) from {{ this }})
    {% endif %}
),

burn_events as (
    select *
    from int_all_transfer
    where to_address = '0x0000000000000000000000000000000000000000'
),

final as (
    select
        -- Keys
        transaction_hash,
        log_index,

        -- Generate date key for partitioning/filtering
        -- TODO: Add block timestamp from blocks table when available
        {{ date_to_integer_key() }} as date_key,

        -- Time dimension
        block_number,
        block_hash,

        -- Contract dimension
        contract_address,

        -- Burn source
        from_address as burned_from_address,

        -- Amount columns
        amount_raw,
        amount as burned_amount,

        -- Metadata
        event_name,
        transaction_index,
        -- _dlt_load_id,

        -- Audit column to track incremental runs
        {{ current_timestamp_func() }} as dbt_loaded_at
    from burn_events
)

select * from final
