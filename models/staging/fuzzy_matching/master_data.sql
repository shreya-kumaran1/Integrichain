{{ config(
    materialized = 'table',
    alias = 'MASTER_DATA'
) }}

with source as (
    -- raw golden entities in SHREYA_SANDBOX.CUSTOMERS
    select * from {{ ref('stg_golden_entities') }}
),

normalized as (
    select
        entity_id as id,
        regexp_replace(
            replace(
                upper(trim(entity_name)) || ' ' ||
                upper(trim(address_line_1)) || ' ' ||
                upper(trim(city)) || ' ' ||
                upper(trim(state)) || ' ' ||
                upper(trim(postal_code)),
                '&', ' AND'
            ),
            '[^A-Z0-9 ]',  -- strip weird chars
            ''
        ) as full_details
    from source
)

select *
from normalized