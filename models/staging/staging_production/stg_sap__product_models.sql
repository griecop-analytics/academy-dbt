with
    product_models_source as (
        select
            cast(productmodelid as string) as product_model_id
            , cast(name as string) as product_model_name    
            --, catalogdescription
            --, instructions        
            --, rowguid
            --, modifieddate
        from {{ source('sap_product', 'productmodel') }}
    )
select *
from product_models_source