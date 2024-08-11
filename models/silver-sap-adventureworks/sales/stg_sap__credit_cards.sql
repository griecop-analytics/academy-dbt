with
    credit_cards_source as (
        select
            cast(creditcardid as string) as credit_card_id
            , cast(cardtype as string) as credit_card_type
            --, cardnumer
            --, expmonth
            --, epyear
            --, modifieddate
        from {{ source('sap', 'creditcard') }}
    )
select *
from credit_cards_source