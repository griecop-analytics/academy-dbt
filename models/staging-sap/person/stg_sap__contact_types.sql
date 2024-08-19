with
    contact_types_source as (
        select
            cast(contacttypeid as string) as contact_type_id
            , cast(name as string) as contact_type_name
            --, modifieddate     
        from {{ source('sap', 'contacttype') }}
    )
select *
from contact_types_source