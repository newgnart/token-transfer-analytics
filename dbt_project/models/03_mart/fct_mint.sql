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


{% if is_incremental() %}
    {% set max_block_query %}
        select COALESCE(MAX(block_number), 0) from {{ this }}
    {% endset %}
    {% set max_block = run_query(max_block_query).columns[0][0] %}
{% endif %}

with int_all_transfer as (
    select * from {{ ref('int_all_transfer') }}
    {% if is_incremental() %}
    -- Only process new blocks since last run
        where block_number > {{ max_block }}
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

        -- Time dimension (needed for incremental)
        block_number,

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