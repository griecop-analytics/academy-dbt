with
    stg_order_sales_reasons as (
        select *
        from {{ ref('stg_sap__soh_sales_reasons') }}
    )

    , stg_sales_reasons as(
        select *
        from {{ ref('stg_sap__sales_reasons') }}
    )

    , join_tables as (
        select
            stg_order_sales_reasons.sales_order_id
            , stg_order_sales_reasons.sales_reason_id
            , stg_sales_reasons.sales_reason_name
            , stg_sales_reasons.sales_reason_type
        from stg_order_sales_reasons
        left join stg_sales_reasons on
            stg_order_sales_reasons.sales_reason_id = stg_sales_reasons.sales_reason_id
    )

select *
from join_tables