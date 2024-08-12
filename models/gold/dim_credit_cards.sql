with
    stg_credit_cards as (
        select *
        from {{ ref('stg_sap__credit_cards') }}
    )

    , sk_generate as (
        select
            sha256(concat('|', credit_card_id)) as credit_card_sk
            , *
        from stg_credit_cards
    )

select *
from sk_generate