with source as (
    select * from {{ source('customers', 'golden_entities') }}
),


cleaned as (
    select
        entity_id,
        -- Basic normalization on entity_name
        upper(trim(entity_name))                       as entity_name_raw,
        regexp_replace(
            upper(trim(entity_name)),
            '[^A-Z0-9 &.-]',
            ''
        )                                              as entity_name_clean,

        -- Basic normalization on address
        upper(trim(address_line_1))                    as address_line_1_raw,
        regexp_replace(
            upper(trim(address_line_1)),
            '[^A-Z0-9 &.-]',
            ''
        )                                               as address_line_1_clean,

        upper(trim(city))                               as city,
        upper(trim(state))                              as state,
        upper(trim(postal_code))                        as postal_code
    from source
),

normalized as (
    select
        entity_id,
        -- Replace & with AND and normalize spaces
        regexp_replace(replace(entity_name_clean, '&', ' AND'), ' +', ' ')        as entity_name_norm,
        regexp_replace(replace(address_line_1_clean, '&', ' AND'), ' +', ' ')     as address_line_1_norm,
        city,
        state,
        postal_code
    from cleaned
)

select
    entity_id,
    entity_name_norm            as entity_name,
    address_line_1_norm         as address_line_1,
    city,
    state,
    postal_code,
    concat_ws(' ',
        entity_name_norm,
        address_line_1_norm,
        city,
        state,
        postal_code
    ) as full_details
from normalized