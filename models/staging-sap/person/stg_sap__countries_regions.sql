with
    countries_regions_source as (
        select
            cast(countryregioncode as string) as country_region_code
            , cast(name as string) as country_region_name
            --, modifieddate   
        from {{ source('sap', 'countryregion') }}
    )
select *
from countries_regions_source