with
    int_sales_reasons as (
        select *
        from {{ ref('int_sales_reasons') }}
    )

    , select_columns as (
        select
            sales_order_id
            , sales_reason_id
            , sales_reason_name
            , sales_reason_type
        from int_sales_reasons
    )

select *
from select_columns