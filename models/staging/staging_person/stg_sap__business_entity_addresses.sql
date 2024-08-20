with
    business_entity_addresses_source as (
        select
            cast(businessentityid as string) as business_entity_id
            , cast(addressid as int) as address_id
            , cast(addresstypeid as string) as address_type_id
            --, modifieddate
            --, rowguide
            , row_number() over (
                partition by businessentityid 
                order by addressid desc)
            as rownum
        from {{ source('sap_person', 'businessentityaddress') }}
    )
    , removed_duplicates as (
        select
            business_entity_id
            , cast(address_id as string) as address_id
            , address_type_id
        from business_entity_addresses_source
        where rownum = 1
    )
select *
from removed_duplicates
