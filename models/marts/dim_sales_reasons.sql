with
    int_sales_reasons as (
        select *
        from {{ ref('int_sales_reasons') }}
    )

    , select_columns as (
        select
            order_id
            , sales_reason_name
            , sales_reason_type
        from int_sales_reasons
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['order_id', 'sales_reason_name']) }} as sales_reason_sk
            , *
        from select_columns
    )

select *
from create_sk