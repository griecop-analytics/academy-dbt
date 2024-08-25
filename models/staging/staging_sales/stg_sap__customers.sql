with
    address_types_source as (
        select
            cast(customerid as string) as customer_id
            , cast(personid as string) as person_id
            , cast(storeid as string) as store_id
            , cast(territoryid as string) as territory_id
        from {{ source('sap_sales', 'customer') }}
    )
select *
from address_types_source
