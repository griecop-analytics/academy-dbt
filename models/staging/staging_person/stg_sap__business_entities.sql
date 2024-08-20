with
    business_entities_source as (
        select
            cast(businessentityid as string) as business_entity_id
            --, rowguid 
            --, modifieddate
        from {{ source('sap_person', 'businessentity') }}
    )
select *
from business_entities_source