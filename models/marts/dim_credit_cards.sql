with
    int_credit_cards as (
        select *
        from {{ ref('int_credit_cards') }}
    )

    , select_columns as (
        select
            order_id
            , credit_card_id
            , payment_method
            , credit_card_type
        from int_credit_cards
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['order_id']) }} as credit_card_sk
            , *
        from select_columns
    )

select *
from create_sk