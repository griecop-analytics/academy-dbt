with
    address_types_source as (
        select
            cast(businessentityid as string) as store_id
            , cast(name as string) as store_name
            , cast(salespersonid as string) as sales_person_id
            --, demographics
            --, modifieddate
            --, rowguide
        from {{ source('sap', 'store') }}
    )
select *
from address_types_source