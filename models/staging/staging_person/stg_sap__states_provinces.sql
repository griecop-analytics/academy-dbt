with
    states_provinces_source as (
        select
            cast(stateprovinceid as string) as state_province_id
            , cast(territoryid as string) as territory_id
            , cast(stateprovincecode as string) as state_province_code
            , cast(countryregioncode as string) as country_region_code
            , cast(name as string) as state_province_name
            , cast(isonlystateprovinceflag as string) as is_state_province
            --, rowguid
            --, modifieddate    
        from {{ source('sap_person', 'stateprovince') }}
    )
select *
from states_provinces_source