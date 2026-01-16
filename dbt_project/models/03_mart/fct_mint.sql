{{
    config(
        materialized='incremental',
        unique_key=['transaction_hash', 'log_index'],
        on_schema_change='fail',
        incremental_strategy='delete+insert'
    )
}}

-- Fact table for Mint events (Transfer events where from_address = 0x0)
-- Built on top of int_all_transfer intermediate model


with int_all_transfer as (
    select * from {{ ref('int_all_transfer') }}
    {% if is_incremental() %}
    -- Only process new blocks since last run
        where block_number > (select COALESCE(MAX(block_number), 0) from {{ this }})
    {% endif %}
),

mint_events as (
    select *
    from int_all_transfer
    where from_address = '0x0000000000000000000000000000000000000000'
),

final as (
    select
        -- Keys for incremental processing
        transaction_hash,
        log_index,

        -- Generate date key for partitioning/filtering
        {{ date_to_integer_key() }} as date_key,

        -- Contract dimension
        contract_address,

        -- Mint destination
        to_address as minting_address,

        -- Amount columns
        amount as minted_amount,

        -- Audit column to track incremental runs
        {{ current_timestamp_func() }} as dbt_loaded_at
    from mint_events
)

select * from final