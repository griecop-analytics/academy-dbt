with
    sales_territory_source as (
        select
            cast(territoryid as string) as territory_id
            , cast(name as string) as territory_name
            , cast(countryregioncode as string) as country_region_code
            , cast(`group` as string) as territory_group
            --, salesytd
            --, saleslastyear
            --, costytd
            --, costlastyear
            --, rowguid
            --, modifieddate
        from {{ source('sap', 'salesterritory') }}
    )
select *
from sales_territory_source