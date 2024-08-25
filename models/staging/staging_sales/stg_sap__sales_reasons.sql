with
    sales_reasons_source as (
        select
            cast(salesreasonid as string) as sales_reason_id
            , cast(name as string) as sales_reason_name
            , cast(reasontype as string) as sales_reason_type
        from {{ source('sap_sales', 'salesreason') }}
    )
select *
from sales_reasons_source
