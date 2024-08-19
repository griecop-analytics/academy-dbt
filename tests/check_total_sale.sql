{{
    config(
        severety = 'error'
    )
}}

with total_sale as (
    select 
        sum(net_sales) as total_net_sales
    from {{ ref('fact_sales') }} 
    where order_date between '2011-01-01' and '2011-12-31'
)

select 
    total_net_sales
from total_sale
where total_net_sales not between 12646000 and 12646100