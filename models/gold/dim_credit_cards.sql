with
    int_credit_cards as (
        select *
        from {{ ref('int_credit_cards') }}
    )

    , select_columns as (
        select
            credit_card_id
            , credit_card_type
        from int_credit_cards
    )

select *
from select_columns