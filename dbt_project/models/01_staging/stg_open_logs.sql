with source as (
    select * from {{ source('raw_data', 'open_logs') }}
),

casted as (
    select
        -- Log metadata
        removed::BOOLEAN as is_removed,
        log_index::BIGINT as log_index,
        transaction_index::BIGINT as transaction_index,
        block_number::BIGINT as block_number,

        -- Convert bytea/binary to hex strings (with 0x prefix) - database agnostic
        {{ bytes_to_hex('transaction_hash') }} as transaction_hash,
        {{ bytes_to_hex('block_hash') }} as block_hash,
        {{ bytes_to_hex('address') }} as contract_address,

        -- Event topics (indexed parameters)
        {{ bytes_to_hex('topic0') }} as topic0,
        {{ bytes_to_hex('topic1') }} as topic1,
        {{ bytes_to_hex('topic2') }} as topic2,

        -- Event data (non-indexed parameters)
        {{ bytes_to_hex('data') }} as event_data

        {%- if target.type == 'postgres' %}
        -- DLT metadata (only available in PostgreSQL environment)
        , _dlt_load_id
        , _dlt_id
        {%- endif %}
    from source
)

select * from casted
