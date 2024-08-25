with
    int_addresses as (
        select *
        from {{ ref('int_addresses') }}
    )

    , stg_territories as(
        select *
        from {{ ref('stg_sap__sales_territories') }}
    )
    
    , join_tables as (
        select distinct
            stg_territories.territory_id
            , stg_territories.territory_name
            , stg_territories.territory_group
            , stg_territories.country_region_code
            , int_addresses.country_region_name
            , int_addresses.state_province_code
            , int_addresses.state_province_name
        from stg_territories
        left join int_addresses 
            on stg_territories.territory_id = int_addresses.territory_id
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['territory_id', 'state_province_code']) }} as territory_sk
            , *
        from join_tables
    )

select * 
from create_sk
