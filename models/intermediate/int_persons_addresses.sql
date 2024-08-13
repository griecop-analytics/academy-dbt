with
    stg_persons as(
        select *
        from {{ ref('stg_sap__persons')}}
    )

    , int_addresses as(
        select *
        from {{ ref('int_addresses') }}
    )

    , join_tables as (
        select
            stg_persons.business_entity_id as person_id
            , int_addresses.address_id
            , concat(first_name,' ', last_name) as full_name
            , case 
                when stg_persons.person_type = 'SC' then 'Store contact'
                when stg_persons.person_type = 'EM' then 'Employee'
                when stg_persons.person_type = 'SP' then 'Sales person'
                when stg_persons.person_type = 'IN' then 'Individual customer'
                when stg_persons.person_type = 'VC' then 'Vendor contact'
                when stg_persons.person_type = 'GC' then 'General contact'
                else stg_persons.person_type
            end as person_type
            , int_addresses.territory_id
            , int_addresses.country_region_code
            , int_addresses.country_region_name
            , int_addresses.state_province_code
            , int_addresses.state_province_name
            , int_addresses.address_city
            , int_addresses.address_type_name
            , int_addresses.street_address
            , int_addresses.postal_code
        from stg_persons
        left join int_addresses on
            stg_persons.business_entity_id = int_addresses.business_entity_id
    )


select *
from join_tables