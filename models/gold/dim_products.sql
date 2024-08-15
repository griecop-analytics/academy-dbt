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
            , product_standardcost 
        from int_products
    )

select *
from select_columns