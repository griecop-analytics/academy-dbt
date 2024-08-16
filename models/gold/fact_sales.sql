with
    int_sales as (
        select *
        from {{ ref('int_sales') }}
    )

    , dim_credit_cards as (
        select *
        from {{ ref('dim_credit_cards') }}
    )

    , dim_customers as (
        select *
        from {{ ref('dim_customers') }}
    )

    , dim_products as (
        select *
        from {{ ref('dim_products') }}
    )

    , dim_sales_persons as (
        select *
        from {{ ref('dim_sales_persons') }}
    )

    , join_tables as (
        select
            int_sales.order_id
            , int_sales.order_detail_id
            , int_sales.territory_id
            , dim_customers.customer_sk as customer_fk
            , dim_sales_persons.sales_person_sk as sales_person_fk
            , dim_credit_cards.credit_card_sk as credit_card_fk
            , int_sales.order_status
            , int_sales.is_online_order
            , int_sales.order_date
            , dim_products.product_sk as product_fk
            , dim_products.product_id
            , int_sales.order_quantity
            , int_sales.product_unit_price
            , dim_products.product_standardcost
            , int_sales.unit_price_discount_pct
            , int_sales.order_subtotal
            , int_sales.order_tax_amount
            , int_sales.order_freight
            , int_sales.order_quantity * int_sales.product_unit_price * (1 - int_sales.unit_price_discount_pct / 100) as net_sales
            , sum(int_sales.order_quantity) over(partition by int_sales.order_id) as total_order_quantity
        from int_sales
        left join dim_credit_cards on
            int_sales.credit_card_id = dim_credit_cards.credit_card_id
        left join dim_customers on  
            int_sales.customer_id = dim_customers.customer_id
        left join dim_products on 
            int_sales.product_id = dim_products.product_id
        left join dim_sales_persons on
            int_sales.sales_person_id = dim_sales_persons.sales_person_id

    )

    , included_metrics as (
        select
            order_id
            , order_detail_id
            , territory_id
            , customer_fk
            , sales_person_fk
            , credit_card_fk
            , product_fk
            , order_status
            , is_online_order
            , order_date
            , order_quantity
            , product_unit_price
            , unit_price_discount_pct
            , order_subtotal
            , order_tax_amount
            , order_freight
            , order_quantity * product_unit_price * (1 - unit_price_discount_pct / 100) as net_sales
            , sum(order_quantity) over(partition by order_id) as total_order_quantity
            , (order_quantity * product_unit_price * (1 - unit_price_discount_pct / 100)/order_subtotal * order_tax_amount) as pondered_tax_amount
            , (order_quantity * product_unit_price * (1 - unit_price_discount_pct / 100)/order_subtotal * order_freight) as pondered_freight
        from join_tables
    )

    , select_columns as (
        select
            order_id
            , order_detail_id
            , territory_id
            , customer_fk
            , sales_person_fk
            , credit_card_fk
            , product_fk
            , order_status
            , is_online_order
            , order_date
            , order_quantity
            , product_unit_price
            , unit_price_discount_pct
            , net_sales
            , pondered_tax_amount
            , pondered_freight
            , (order_quantity * product_unit_price + pondered_tax_amount + pondered_freight) as gross_sales
        from included_metrics
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['order_id', 'order_detail_id']) }} as sales_sk
            , *
        from select_columns
    )

select *
from create_sk