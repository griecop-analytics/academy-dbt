with
    product_subcategories_source as (
        select
            cast(productsubcategoryid as string) as product_subcategory_id
            , cast(productcategoryid as string) as product_category_id
            , cast(name as string) as product_subcategory_name            
            --, rowguid
            --, modifieddate
        from {{ source('sap', 'productsubcategory') }}
    )
select *
from product_subcategories_source