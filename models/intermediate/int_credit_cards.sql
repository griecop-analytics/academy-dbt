with
    stg_sales_order as (
        select *
        from {{ ref('stg_sap__sales_order_header') }}
    )

    , stg_credit_cards as (
        select *
        from {{ ref('stg_sap__credit_cards') }}
    )

    , join_tables as (
        select
            stg_sales_order.order_id
            , stg_sales_order.credit_card_id
            , case 
                when stg_sales_order.credit_card_id is null
                    then 'Other Payment Method'
                else 'Credit Card Payment'
            end as payment_method
            , coalesce(credit_card_type, 'No credit card used') as credit_card_type
        from stg_sales_order
        left join stg_credit_cards on  
            stg_sales_order.credit_card_id = stg_credit_cards.credit_card_id
    )

select *
from join_tables