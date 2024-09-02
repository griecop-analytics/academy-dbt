with
    sales_persons_source as (
        select
            cast(businessentityid as string) as sales_person_id
            , cast(territoryid as string) as territory_id
            , cast(commissionpct as decimal) as commission_pct
        from {{ source('sap_sales', 'salesperson') }}
    )
select *
from sales_persons_source