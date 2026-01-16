{{ config( materialized='ephemeral' ) }}

-- Intermediate table for all Transfer events extracted from blockchain event logs
-- Decodes Transfer(address,address,uint256) events from raw logs


with stg_open_logs as (
    select * from {{ ref('stg_open_logs') }}
),

int_event as (
    select * from {{ ref('int_event') }}
),

-- Filter for Transfer events only
transfer_events as (
    select
        logs.*,
        evt.event_name
    from
        stg_open_logs as logs
    inner join int_event as evt on logs.topic0 = evt.topic0_hash
    where
        evt.event_name = 'Transfer'
        and logs.is_removed = false
),

-- Decode Transfer event parameters
-- Transfer(address indexed from, address indexed to, uint256 value)
-- topic0 = event signature hash
-- topic1 = from address (indexed)
-- topic2 = to address (indexed)
-- data = value (uint256, non-indexed)
decoded as (
    select
        -- Keys
        transaction_hash,
        log_index,

        -- Time dimension
        block_number,
        block_hash,

        -- Contract dimension
        contract_address,

        -- Transfer parameters (decode from topics and data)
        -- Remove 0x prefix and leading zeros, then add 0x prefix for addresses
        '0x' || {{ substring_from('topic1', 27) }} as from_address, -- topic1 is 66 chars (0x + 64 hex), address is last 40 hex chars
        '0x' || {{ substring_from('topic2', 27) }} as to_address, -- topic2 is 66 chars (0x + 64 hex), address is last 40 hex chars

        -- Convert hex value to numeric
        -- data is 0x followed by 64 hex chars (256 bits)
        -- Remove 0x prefix and leading zeros, then convert to bigint
        case
            when
                event_data = '0x'
                or event_data is null
                or LENGTH({{ trim_leading_zeros("SUBSTRING(event_data, 3)") }}) = 0 then 0
            else {{ hex_to_bigint(trim_leading_zeros("SUBSTRING(event_data, 3)")) }}
        end as amount_raw,

        -- Event metadata
        event_name,
        transaction_index

        {%- if target.type == 'postgres' %}
            ,
            -- DLT metadata (only available in PostgreSQL environment)
            _dlt_load_id,
            _dlt_id
        {%- endif %}

    from transfer_events
),

final as (
    select
        -- Keys
        transaction_hash,
        log_index,

        -- Time dimension
        block_number, block_hash,

        -- Contract dimension
        contract_address,

        -- Address dimensions
        from_address, to_address,

        -- Amount (raw value before decimal adjustment)
        amount_raw,

        -- Convert to decimal assuming 18 decimals (standard ERC20)
        -- TODO: Join with token metadata to get actual decimals
        (amount_raw / POWER(10, 18)) as amount,

        -- Metadata
        event_name, transaction_index
        {%- if target.type == 'postgres' %}
            , _dlt_load_id, _dlt_id
        {%- endif %}
    from decoded
)

select * from final
