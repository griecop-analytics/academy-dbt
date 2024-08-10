with
    business_entity_addresses_source as (
        select
            cast(businessentityid as int) as business_entity_id
            , cast(addressid as int) as address_id
            , cast(addresstypeid as string) as address_type_id
            , date(modifieddate) as modified_date
            --, rowguide
        from {{ source('sap', 'businessentityaddress') }}
    )
select *
from business_entity_addresses_source