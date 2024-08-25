with
    products_source as (
        select
            cast(productid as string) as product_id
            , cast(productsubcategoryid as string) as product_subcategory_id
            , cast(productmodelid as string) as product_model_id
            , cast(name as string) as product_name
            , cast(productnumber as string) as product_number
            , cast(makeflag as string) as is_product_manufactured
            , cast(standardcost as decimal) as product_standardcost
            , cast(size as string) as product_size
            , cast(sizeunitmeasurecode as string) as size_unitmeasure_code
            , cast(weightunitmeasurecode as string) as weight_unitmeasure_code
            , cast(weight  as decimal) as product_weight
            , cast(daystomanufacture  as int) as days_to_manufacture
            , cast(productline as string) as product_line
            , cast(class  as string) as product_class
            , cast(style as string) as product_style
            , date(sellstartdate) as product_sellstart_date
            , date(sellenddate) as product_sellend_date
            , cast(finishedgoodsflag as string) as is_product_salable
        from {{ source('sap_product', 'product') }}
    )
select *
from products_source