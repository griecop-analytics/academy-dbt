with
    sales_orders_header_source as (
        select
            cast(salesorderid as string) as order_id
            , cast(customerid as string) as customer_id
            , cast(salespersonid as string) as sales_person_id
            , cast(territoryid as string) as territory_id
            , cast(billtoaddressid as string) as bill_to_address_id
            , cast(shiptoaddressid as string) as ship_to_address_id
            , cast(shipmethodid as string) as ship_method_id
            , cast(creditcardid as string) as credit_card_id
            , date(orderdate) as order_date
            , date(duedate) as order_due_date
            , date(shipdate) as order_ship_date
            , cast(status as string) as order_status
            , cast(onlineorderflag as string) as is_online_order
            , cast(subtotal as numeric) as order_subtotal
            , cast(taxamt as numeric) as order_tax_amount
            , cast(freight as numeric) as order_freight
            , cast(totaldue as numeric) as order_total_due
        from {{ source('sap_sales', 'salesorderheader') }}
    )
select *
from sales_orders_header_source
