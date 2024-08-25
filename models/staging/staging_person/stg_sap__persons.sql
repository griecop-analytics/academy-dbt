with
    persons_source as (
        select
            cast(businessentityid as string) as business_entity_id
            , cast(persontype as string) as person_type
            , cast(firstname as string) as first_name
            , cast(middlename as string) as middle_name
            , cast(lastname as string) as last_name
        from {{ source('sap_person', 'person') }}
    )
select *
from persons_source