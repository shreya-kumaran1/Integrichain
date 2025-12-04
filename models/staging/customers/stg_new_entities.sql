with source as (
    select * from {{ source('customers', 'new_entities') }}
),

cleaned as (
    select
        new_entity_id,
        -- this is the "true" golden id in our synthetic data, useful for eval
        entity_id                            as golden_entity_id_if_known,

        upper(trim(entity_name))             as entity_name_raw,
        regexp_replace(
            upper(trim(entity_name)),
            '[^A-Z0-9 &.-]',
            ''
        )                                     as entity_name_clean,

        upper(trim(address_line_1))           as address_line_1_raw,
        regexp_replace(
            upper(trim(address_line_1)),
            '[^A-Z0-9 &.-]',
            ''
        )                                     as address_line_1_clean,

        upper(trim(city))                     as city,
        upper(trim(state))                    as state,
        upper(trim(postal_code))              as postal_code
    from source
),

normalized as (
    select
        new_entity_id,
        golden_entity_id_if_known,
        regexp_replace(replace(entity_name_clean, '&', ' AND'), ' +', ' ')        as entity_name_norm,
        regexp_replace(replace(address_line_1_clean, '&', ' AND'), ' +', ' ')     as address_line_1_norm,
        city,
        state,
        postal_code
    from cleaned
)

select
    new_entity_id,
    golden_entity_id_if_known,
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