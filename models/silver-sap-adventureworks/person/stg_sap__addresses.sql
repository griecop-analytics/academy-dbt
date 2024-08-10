with
    addresses_source as (
        select
            cast(addressid as string) as address_id
            , cast(stateprovinceid as string) as state_province_id
            , cast(addressline1 as string) as street_address
            , cast(city as string) as address_city
            , cast(postalcode as string) as postal_code
            --, rowguid 
            --, modifieddate
            --, addressline2
            --, spatiallocation      
        from {{ source('sap', 'address') }}
    )
select *
from addresses_source