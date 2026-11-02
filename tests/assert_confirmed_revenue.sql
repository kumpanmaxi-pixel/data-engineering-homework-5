with source_confirmed as (
    select
        o.order_id,
        o.total_amount
    from {{ source('shop', 'orders') }} o
    where o.status = 'paid'
      and exists (
          select 1
          from {{ source('shop', 'payments') }} p
          where p.order_id = o.order_id
            and p.payment_status = 'success'
            and p.amount > 0
      )
),
source_stats as (
    select
        count(*) as orders_count,
        coalesce(sum(total_amount), 0) as total_revenue
    from source_confirmed
),
mart_stats as (
    select
        count(*) as orders_count,
        coalesce(sum(order_amount), 0) as total_revenue
    from {{ ref('fct_orders') }}
    where is_confirmed
)
select
    s.orders_count as source_orders_count,
    m.orders_count as mart_orders_count,
    s.total_revenue as source_revenue,
    m.total_revenue as mart_revenue
from source_stats s
cross join mart_stats m
where s.orders_count <> m.orders_count
   or s.total_revenue <> m.total_revenue