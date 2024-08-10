with
    product_categories_source as (
        select
            cast(productcategoryid as string) as product_category_id
            , cast(name as string) as product_category_name 
        from {{ source('sap', 'productcategory') }}
    )
select *
from product_categories_source