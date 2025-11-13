with source as (
    select * from {{ source('raw_data', 'raw_transfer') }}
),

casted as (
    select
        id,
        block_number::BIGINT as block_number,
        {% if target.type == 'postgres' %}
            TO_TIMESTAMP(timestamp::BIGINT) at time zone 'UTC' as block_timestamp,
        {% else %}
            TO_TIMESTAMP(timestamp::BIGINT) as block_timestamp,
        {% endif %}
        contract_address::VARCHAR(42) as contract_address,
        {% if target.type == 'postgres' %}
            "from"::VARCHAR(42) as from_address,
            "to"::VARCHAR(42) as to_address,
        {% else %}
            "FROM"::VARCHAR(42) as from_address,
            "TO"::VARCHAR(42) as to_address,
        {% endif %}
        value::NUMERIC(38, 0) as amount_raw,
        _dlt_load_id,
        _dlt_id
    from source
)

select * from casted
