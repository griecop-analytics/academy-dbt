with
    sales_order_details_source as (
        select
            cast(salesorderid as string) as order_id
            , cast(salesorderdetailid as string) as order_detail_id
            , cast(productid as string) as product_id
            , cast(orderqty as integer) as order_quantity
            , cast(unitprice as numeric) as product_unit_price
            , cast(unitpricediscount as numeric) as unit_price_discount_pct
            --, specialofferid
            --, carriertrackingnumber
            --, modifieddate
            --, rowguide
        from {{ source('sap', 'salesorderdetail') }}
    )
select *
from sales_order_details_source
