with
    address_types_source as (
        select
            cast(addresstypeid as string) as address_type_id
            , cast(name as string) as address_type_name
            --, modifieddate
            --, rowguide
        from {{ source('sap_person', 'addresstype') }}
    )
select *
from address_types_source