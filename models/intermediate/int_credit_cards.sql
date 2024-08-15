with
    stg_credit_cards as (
        select *
        from {{ ref('stg_sap__credit_cards') }}
    )

    , select_columns as (
        select  
            credit_card_id
            , credit_card_type
        from stg_credit_cards
    )

select *
from select_columns