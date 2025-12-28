-- Event signature dimension table
-- Maps event signature hashes to human-readable event names and signatures


with int_event as (
    select * from {{ ref('int_event') }}
),

final as (
    select
        -- Primary key
        topic0_hash,

-- Event attributes
event_name, event_signature,

-- Parse event components (e.g., "Transfer(address,address,uint256)")
SPLIT_PART(event_signature, '(', 1) as event_base_name,
SPLIT_PART(
    SPLIT_PART(event_signature, '(', 2),
    ')',
    1
) as event_parameters,

-- Categorize common event types
case
    when event_name = 'Transfer' then 'token_transfer'
    when event_name = 'Approval' then 'token_approval'
    when event_name IN (
        'RoleGranted',
        'RoleRevoked',
        'RoleAdminChanged'
    ) then 'access_control'
    when event_name IN (
        'Upgraded',
        'AdminChanged',
        'Initialized'
    ) then 'contract_upgrade'
    when event_name LIKE 'Auction%' then 'auction'
    when event_name LIKE 'Rebalance%' then 'rebalancing'
    else 'other'
end as event_category

from int_event )

select * from final