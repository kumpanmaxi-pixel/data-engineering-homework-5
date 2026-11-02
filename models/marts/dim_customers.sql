{{ config(
    materialized='table',
    distributed_replicated=true
) }}
with confirmed_orders as (
    select
        order_id,
        customer_id,
        first_paid_at,
        net_paid_amount,
        row_number() over (
            partition by customer_id
            order by first_paid_at, order_id
        ) as purchase_number
    from {{ ref('fct_orders') }}
    where is_confirmed
),
customer_stats as (
    select
        customer_id,
        max(case when purchase_number = 1 then order_id end)
            as first_confirmed_order_id,
        min(first_paid_at) as first_confirmed_at,
        count(*) as confirmed_orders_count,
        sum(net_paid_amount) as confirmed_revenue
    from confirmed_orders
    group by customer_id
)
select
    c.customer_id,
    c.registered_at,
    c.registration_date,
    c.city,
    c.acquisition_channel,
    s.first_confirmed_order_id,
    s.first_confirmed_at,
    coalesce(s.confirmed_orders_count, 0) as confirmed_orders_count,
    coalesce(s.confirmed_revenue, 0) as confirmed_revenue,
    coalesce(s.confirmed_orders_count, 0) > 0 as is_buyer
from {{ ref('stg_shop__customers') }} c
left join customer_stats s
    on c.customer_id = s.customer_id