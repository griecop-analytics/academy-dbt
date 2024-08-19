with
    product_standard_cost as (
        select
            cast(productid as string) as product_id
            , date(startdate) as start_date
            , coalesce(date(enddate), '2029-05-30') as end_date
            , cast(standardcost as numeric) as product_standard_cost
        from {{ source('sap', 'productcosthistory') }}
        order by product_id, start_date
    )

select *
from product_standard_cost