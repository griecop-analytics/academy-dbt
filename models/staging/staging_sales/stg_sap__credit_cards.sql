with
    credit_cards_source as (
        select
            cast(creditcardid as string) as credit_card_id
            , cast(cardtype as string) as credit_card_type
        from {{ source('sap_sales', 'creditcard') }}
    )
select *
from credit_cards_source