with
    stg_orders as(
        select *
        from {{ ref('stg_sap__sales_order_header') }}
    )

    , stg_order_sales_reasons as (
        select *
        from {{ ref('stg_sap__soh_sales_reasons') }}
    )

    , stg_sales_reasons as(
        select *
        from {{ ref('stg_sap__sales_reasons') }}
    )

    , join_tables as (
        select
            stg_orders.order_id
            , stg_order_sales_reasons.sales_reason_id
            , stg_sales_reasons.sales_reason_name
            , stg_sales_reasons.sales_reason_type
        from stg_orders
        left join stg_order_sales_reasons on
            stg_orders.order_id = stg_order_sales_reasons.sales_order_id
        left join stg_sales_reasons on      
            stg_order_sales_reasons.sales_reason_id = stg_sales_reasons.sales_reason_id
    )

    , aggregate_columns as (
        select
            order_id
            , string_agg(sales_reason_name, ', ') agg_sales_reason_name
		    , string_agg(sales_reason_type , ', ') as agg_sales_reason_type
        from join_tables   
        group by order_id
    )

    , cleaned_data as (
        select
            order_id
            , coalesce(
                regexp_replace(agg_sales_reason_name, ', Other', ''),
                'Unspecified reason'
             ) as sales_reason_name
            , case
                when agg_sales_reason_type = 'Other, Promotion' then 'Promotion'
                when agg_sales_reason_type = 'Other, Promotion, Other' then 'Promotion'
                when agg_sales_reason_type = 'Other, Other' then 'Other'
                when agg_sales_reason_type = 'Marketing, Other' then 'Marketing'
                when agg_sales_reason_type = 'Promotion, Other' then 'Promotion'
                when agg_sales_reason_type is null then 'Unspecified reason'
                else agg_sales_reason_type
            end as sales_reason_type
        from aggregate_columns
    )

select *
from cleaned_data