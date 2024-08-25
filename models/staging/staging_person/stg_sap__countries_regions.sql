with
    countries_regions_source as (
        select
            cast(countryregioncode as string) as country_region_code
            , cast(name as string) as country_region_name
        from {{ source('sap_person', 'countryregion') }}
    )
select *
from countries_regions_source