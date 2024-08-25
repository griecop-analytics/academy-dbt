with
    soh_sales_reasons_source as (
        select
            cast(salesorderid as string) as sales_order_id
            , cast(salesreasonid as string) as sales_reason_id
        from {{ source('sap_sales', 'salesorderheadersalesreason') }}
    )
select *
from soh_sales_reasons_source
