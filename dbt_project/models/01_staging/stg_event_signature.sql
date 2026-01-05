with source as (
    select * from {{ source('raw_data', 'event_signature') }}
),

casted as (
    select
        topic0::VARCHAR(66) as topic0_hash,
        event_name::VARCHAR(100) as event_name,
        signature::VARCHAR(255) as event_signature

        {%- if target.type == 'postgres' %}
        -- DLT metadata (only available in PostgreSQL environment)
            , _dlt_load_id,
            _dlt_id
        {%- endif %}
    from source
)

select * from casted
