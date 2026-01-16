{{ config(materialized='view') }}

-- Debug model to check hex conversion
-- Run: dbt run --select debug_conversion --target prod

with sample_data as (
    select
        event_data,
        topic0
    from {{ ref('stg_open_logs') }}
    where event_data is not null
      and event_data != '0x'
      and topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    limit 20
),

parsed as (
    select
        event_data,
        LENGTH(event_data) as data_len,
        SUBSTRING(event_data, 3) as without_0x,
        {{ trim_leading_zeros("SUBSTRING(event_data, 3)") }} as trimmed,
        LENGTH({{ trim_leading_zeros("SUBSTRING(event_data, 3)") }}) as trimmed_len,

        -- What the macro produces
        {{ hex_to_bigint(trim_leading_zeros("SUBSTRING(event_data, 3)")) }} as amount_raw,

        -- Final amount with 18 decimals
        {{ hex_to_bigint(trim_leading_zeros("SUBSTRING(event_data, 3)")) }} / POWER(10, 18) as amount
    from sample_data
)

select * from parsed
