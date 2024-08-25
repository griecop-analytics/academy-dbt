with
    business_entity_contacts_source as (
        select
            cast(businessentityid as string) as business_entity_id
            , cast(personid as string) as person_id
            , cast(contacttypeid as string) as contact_type_id
        from {{ source('sap_person', 'businessentitycontact') }}
    )
select *
from business_entity_contacts_source
