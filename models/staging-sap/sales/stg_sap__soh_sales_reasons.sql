with
    soh_sales_reasons_source as (
        select
            cast(salesorderid as string) as sales_order_id
            , cast(salesreasonid as string) as sales_reason_id
            --, modifieddate
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )

select *
from soh_sales_reasons_source
