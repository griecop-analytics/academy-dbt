with
    business_entities_source as (
        select
            cast(businessentityid as string) as business_entity_id
            --, rowguid 
            --, modifieddate
        from {{ source('sap', 'businessentity') }}
    )
select *
from business_entities_source