select
    o.order_id,
    o.order_amount,
    sum(i.line_amount) as items_amount
from {{ ref('fct_orders') }} o
join {{ ref('fct_order_items') }} i
    on o.order_id = i.order_id
group by o.order_id, o.order_amount
having abs(o.order_amount - sum(i.line_amount)) > 0.01