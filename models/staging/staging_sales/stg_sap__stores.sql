with
    address_types_source as (
        select
            cast(businessentityid as string) as store_id
            , cast(name as string) as store_name
            , cast(salespersonid as string) as sales_person_id
        from {{ source('sap_sales', 'store') }}
    )
select *
from address_types_source