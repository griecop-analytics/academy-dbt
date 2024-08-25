with
    product_subcategories_source as (
        select
            cast(productsubcategoryid as string) as product_subcategory_id
            , cast(productcategoryid as string) as product_category_id
            , cast(name as string) as product_subcategory_name            
        from {{ source('sap_product', 'productsubcategory') }}
    )
select *
from product_subcategories_source