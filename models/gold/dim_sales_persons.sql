with
    stg_sales_persons as (
        select *
        from {{ ref('stg_sap__sales_persons') }}
    )

    , int_persons_addresses as(
        select *
        from {{ ref('int_persons_addresses') }}
    )

    , stg_sales_territories as (
        select *
        from {{ ref('stg_sap__sales_territories')}}
    )

    , join_tables as (
        select
            stg_sales_persons.sales_person_id
            , int_persons_addresses.full_name
            , int_persons_addresses.person_type
            , stg_sales_persons.commission_pct 
            , stg_sales_persons.territory_id
            , stg_sales_territories.territory_name
            , stg_sales_territories.country_region_code
            , stg_sales_territories.territory_group
        from stg_sales_persons
        left join int_persons_addresses on
            stg_sales_persons.sales_person_id = int_persons_addresses.person_id
        left join stg_sales_territories on
            stg_sales_persons.territory_id = stg_sales_territories.territory_id
        )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_person_id']) }} as sales_person_sk
            , *
        from join_tables
    )

select *
from create_sk