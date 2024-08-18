with
    int_sales_persons as (
        select *
        from {{ ref('int_sales_persons') }}
    )

    , select_columns as (
        select
            sales_person_id
            , full_name
            , person_type
            , commission_pct 
            , territory_id
            , territory_name
            , country_region_code
            , territory_group
        from int_sales_persons
        )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['sales_person_id']) }} as sales_person_sk
            , *
        from select_columns
    )

select *
from create_sk