with
    stg_products as (
        select *
        from {{ ref('stg_sap__products') }}
    )

    , stg_models as(
        select *
        from {{ ref('stg_sap__product_models')}}
    )

    , stg_subcategories as(
        select *
        from {{ ref('stg_sap__product_subcategories') }}
    )

    , stg_categories as (
        select *
        from {{ ref('stg_sap__product_categories') }}
    )

    , join_tables as (
        select 
            stg_products.product_id
            , stg_subcategories.product_subcategory_id
            , stg_categories.product_category_id
            , stg_models.product_model_id
            , stg_products.product_name
            , stg_models.product_model_name
            , stg_subcategories.product_subcategory_name
            , stg_categories.product_category_name
            , stg_products.product_size
            , stg_products.size_unitmeasure_code
            , stg_products.weight_unitmeasure_code
            , stg_products.product_weight
            , stg_products.product_class
            , stg_products.product_style
            , stg_products.product_standardcost
            , stg_products.product_sellstart_date
            , stg_products.product_sellend_date
            , stg_products.is_product_salable
        from stg_products
        left join stg_models on
            stg_products.product_model_id = stg_models.product_model_id
        left join stg_subcategories on
            stg_products.product_subcategory_id = stg_subcategories.product_subcategory_id
        left join stg_categories on
            stg_subcategories.product_category_id = stg_categories.product_category_id
        )

select *
from join_tables
where is_product_salable = 'true'