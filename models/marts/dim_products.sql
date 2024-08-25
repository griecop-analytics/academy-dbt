with
    int_products as (
        select *
        from {{ ref('int_products') }}
    )

    , select_columns as (
        select
            product_id
            , product_subcategory_id
            , product_category_id
            , product_model_id
            , product_name
            , product_model_name
            , product_subcategory_name
            , product_category_name
        from int_products
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk
            , *
        from select_columns
    )

select *
from create_sk