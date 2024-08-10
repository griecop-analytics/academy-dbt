with
    stores_source as (
        select
            cast(businessentityid as string) as store_id
            , cast(name as string) as store_name
            , cast(salespersonid as string) as sales_person_id
            --, demographics
            --, rowguide
            --,modifieddate
        from {{ source('sap', 'store') }}
    )
select *
from stores_source
