with source as (
    select * from {{ source('raw_data', 'raw_stablecoin') }}
),

casted as (
    select
        contract_address::VARCHAR(42) as contract_address,
        chain::VARCHAR(20) as chain,
        symbol::VARCHAR(20) as symbol,
        name::VARCHAR(100) as name,
        currency::VARCHAR(10) as currency,
        backing_type::VARCHAR(20) as backing_type,
        decimals::INTEGER as decimals
    from source
)

select * from casted
