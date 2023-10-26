-- payment created date should not be early than order date
with
    orders as (select * from {{ ref("stg_orders") }}),

    payments as (select * from {{ ref("stg_payments") }})

select payments.payment_id, payments.created_date, orders.order_date
from payments
left join orders using (order_id)
where payments.created_date < orders.order_date
