with
    stg_customers as(
        select *
        from {{ ref('stg_sap__customers')}}
        where person_id is not null
    )

    , int_persons as(
        select *
        from {{ ref('int_persons_addresses') }}
    )

    , int_addresses as(
        select *
        from {{ ref('int_addresses') }}
    )

    , stg_stores as(
        select *
        from {{ ref('stg_sap__stores') }}
    )

    , join_tables as (
        select
            stg_customers.customer_id
            , int_persons.person_id
            , stg_stores.store_id
            , stg_stores.sales_person_id
            , stg_stores.store_name
            , int_persons.full_name
            , int_persons.person_type
            , stg_customers.territory_id
            , int_addresses.country_region_code
            , int_addresses.country_region_name
            , int_addresses.state_province_code
            , int_addresses.state_province_name
            , int_addresses.address_city
        from stg_customers
        left join int_persons on
            stg_customers.person_id = int_persons.person_id
        left join stg_stores on
            stg_customers.store_id = stg_stores.store_id
        left join int_addresses on 
            stg_customers.person_id = int_addresses.business_entity_id or
            stg_customers.store_id = int_addresses.business_entity_id 
    )


select *
from join_tables
