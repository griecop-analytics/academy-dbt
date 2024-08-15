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

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['credit_card_id']) }} as credit_card_sk
            , *
        from select_columns
    )

select *
from create_sk