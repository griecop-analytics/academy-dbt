with
    stg_order_details as (
        select *
        from {{ ref('stg_sap__sales_order_detail') }}
    )

    , stg_orders as(
        select *
        from {{ ref('stg_sap__sales_order_header') }}
    )

    , join_tables as (
        select 
            stg_orders.order_id
            , stg_order_details.order_detail_id
            , stg_orders.territory_id
            , stg_orders.customer_id
            , stg_orders.sales_person_id
            , stg_orders.credit_card_id
            , stg_orders.order_status
            , stg_orders.is_online_order
            , stg_orders.order_date
            , stg_order_details.product_id
            , stg_order_details.order_quantity
            , stg_order_details.product_unit_price
            , stg_order_details.unit_price_discount_pct
            , stg_orders.order_subtotal
            , stg_orders.order_tax_amount
            , stg_orders.order_freight
            , stg_orders.order_total_due
        from stg_order_details
        left join stg_orders on
            stg_order_details.order_id = stg_orders.order_id
        order by order_id, order_detail_id
    )

select *
from join_tables
