with
    stg_business_entity_addresses as (
        select *
        from {{ ref('stg_sap__business_entity_addresses') }}
    )

    , stg_addresses as(
        select *
        from {{ ref('stg_sap__addresses') }}
    )

    , stg_address_types as(
        select *
        from {{ ref('stg_sap__address_types') }}
    )

    , stg_state_provinces as (
        select *
        from {{ ref('stg_sap__states_provinces') }}
    )

    , stg_country_regions as (
        select *
        from {{ ref('stg_sap__countries_regions') }}
    )

    , join_tables as (
        select 
            stg_addresses.address_id
            , stg_business_entity_addresses.business_entity_id
            , stg_addresses.state_province_id
            , stg_state_provinces.territory_id
            , stg_country_regions.country_region_code
            , stg_state_provinces.state_province_code
            , stg_country_regions.country_region_name
            , stg_state_provinces.state_province_name            
            , stg_address_types.address_type_name
            , stg_addresses.street_address
            , stg_addresses.address_city
            , stg_addresses.postal_code
        from stg_business_entity_addresses
        left join stg_addresses 
            on stg_business_entity_addresses.address_id = stg_addresses.address_id
        left join stg_address_types 
            on stg_business_entity_addresses.address_type_id = stg_address_types.address_type_id
        left join stg_state_provinces 
            on stg_addresses.state_province_id = stg_state_provinces.state_province_id
        left join stg_country_regions 
            on stg_state_provinces.country_region_code = stg_country_regions.country_region_code
    )

select *
from join_tables
