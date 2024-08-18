with
    int_customers as (
        select *
        from {{ ref('int_customers') }}
    )

    , select_columns as (
        select
            customer_id
            , person_id
            , store_id
            , sales_person_id
            , case
                when store_id is null
                    then full_name
                else store_name
            end as customer_name
            , person_type
            , territory_id
            , country_region_code
            , country_region_name
            , state_province_code
            , state_province_name
            , address_city
        from int_customers
    )

    , create_sk as (
        select
            {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk
            , *
        from select_columns
    )

select *
from create_sk