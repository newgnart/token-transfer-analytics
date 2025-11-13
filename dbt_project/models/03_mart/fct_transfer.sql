{{
    config(
        materialized='incremental',
        unique_key=['transaction_hash', 'log_index'],
        on_schema_change='fail',
        incremental_strategy='delete+insert'
    )
}}


with stg_transfer as (
    select * from {{ ref('stg_transfer') }}
    {% if is_incremental() %}
    -- Only process new blocks since last run
        where block_number >= (select COALESCE(MAX(block_number), 0) as max_block from {{ this }})
    {% endif %}
),

dim_stablecoin as (
    select * from {{ ref('dim_stablecoin') }}
    where is_current = true  -- Only use current stablecoin metadata
),

parsed as (
    select
        -- Parse natural key from id (format: "0xtxhash_logindex")
        SPLIT_PART(id, '_', 2)::INTEGER as log_index,
        TO_CHAR(block_timestamp, 'YYYYMMDD')::INTEGER as date_key,

        -- Time dimension
        block_number,
        block_timestamp,
        contract_address,

        -- Contract/token dimension
        'ethereum' as chain,
        from_address, -- TODO: get chain from raw data when available

        -- Address dimensions

        to_address,
        SPLIT_PART(id, '_', 1) as transaction_hash

    from stg_transfer
),

enriched as (
    select
        -- Keys
        p.transaction_hash,
        p.log_index,

        -- Time dimension
        p.date_key,
        p.block_number,
        p.block_timestamp,

        -- Contract/token dimension
        p.contract_address,
        p.chain,

        -- Address dimensions
        p.from_address,
        p.to_address,

        -- Join stablecoin metadata
        d.symbol,
        d.name,
        COALESCE(d.decimals, 18) as decimals,

        -- Determine transaction type
        case
            when p.from_address = '0x0000000000000000000000000000000000000000' then 'mint'
            when p.to_address = '0x0000000000000000000000000000000000000000' then 'burn'
            else 'transfer'
        end as transaction_type,

        -- Convert to decimal amount using actual decimals from dim_stablecoin
        -- For stablecoins, amount ≈ USD value. TODO: have dim_price table

        {{ convert_token_amount('s.amount_raw', 'COALESCE(d.decimals, 18)', 2) }} as amount

    from parsed as p
    left join stg_transfer as s
        on
            p.transaction_hash = SPLIT_PART(s.id, '_', 1)
            and p.log_index = SPLIT_PART(s.id, '_', 2)::INTEGER
    left join dim_stablecoin as d
        on
            LOWER(p.contract_address) = LOWER(d.contract_address)
            and p.chain = d.chain
)

select
    transaction_hash,
    log_index,
    date_key,
    block_number,
    block_timestamp,
    contract_address,
    chain,
    from_address,
    to_address,
    symbol,
    name,
    decimals,
    transaction_type,
    amount,

    -- Audit column to track incremental runs
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) as dbt_loaded_at
from enriched
