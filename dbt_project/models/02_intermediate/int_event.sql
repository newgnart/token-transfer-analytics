{{ config( materialized='ephemeral' ) }}

-- Intermediate model for event signatures
-- Simple passthrough from staging to be used by other intermediate models

with stg_event_signature as (
    select * from {{ ref('stg_event_signature') }}
),

final as (
    select
        topic0_hash,
        event_name,
        event_signature
    from stg_event_signature
)

select * from final
