{{ config(
    materialized = 'table',
    alias = 'CANDIDATE_DATA'
) }}

with source as (
    -- raw "new entities" in SHREYA_SANDBOX.CUSTOMERS
    select * from {{ source('customers', 'new_entities') }}
),

normalized as (
    select
        new_entity_id as id,

        regexp_replace(
            replace(
                upper(trim(entity_name)) || ' ' ||
                upper(trim(address_line_1)) || ' ' ||
                upper(trim(city)) || ' ' ||
                upper(trim(state)) || ' ' ||
                upper(trim(postal_code)),
                '&', ' AND'
            ),
            '[^A-Z0-9 ]',
            ''
        ) as full_details
    from source
)

select *
from normalized