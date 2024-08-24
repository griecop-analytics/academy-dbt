with
    fact_sales as (
        select *
        from {{ ref('fact_sales') }}
    )

    , agg_columns as (
        select
            fact_sales.sales_person_fk
            , fact_sales.territory_fk
            , count(distinct fact_sales.order_id) as total_orders
            , fact_sales.order_date
            , sum(fact_sales.order_quantity) as total_products
            , sum(fact_sales.net_sales) as net_sales_total
            , sum(fact_sales.gross_sales) as gross_sales_total
            , sum(fact_sales.due_sales) as due_sales_total
        from fact_sales
        where sales_person_fk is not null
        group by sales_person_fk, territory_fk, order_date
    )

    , correct_reference_month as (
        select
            *
            , format_date('%b-%Y', date_sub(order_date, interval 1 day)) as reference_month_year
        from agg_columns
    )
    
    , final_query as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_person_fk', 'order_date', 'territory_fk']) }} as sales_salespersons_period_sk 
           , sales_person_fk 
           , territory_fk
           , order_date
           , reference_month_year
           , total_orders
           , total_products
           , net_sales_total
           , gross_sales_total
           , due_sales_total
        from correct_reference_month
    )

select *
from final_query
