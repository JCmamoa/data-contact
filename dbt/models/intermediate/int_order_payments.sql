{{ config(materialized='ephemeral') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments_agg as (
    select
        order_id,
        count(*) as payment_count,
        sum(case when payment_status = 'succeeded' then amount_paid else 0 end) as total_amount_paid,
        sum(case when payment_status = 'succeeded' then gateway_fee else 0 end) as total_gateway_fee,
        max(payment_method) as payment_method,
        max(payment_status) as payment_status,
        max(paid_at) as paid_at,
        max(updated_at) as last_payment_updated_at
    from {{ ref('stg_payments') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.gross_amount,
    o.discount_amount,
    o.tax_amount,
    coalesce(p.total_gateway_fee, 0) as gateway_fee,
    coalesce(p.total_amount_paid, 0) as amount_paid,
    {{ calculate_net_revenue('o.gross_amount', 'o.discount_amount', 'o.tax_amount', 'coalesce(p.total_gateway_fee, 0)') }} as net_revenue,
    o.order_status,
    o.currency,
    coalesce(p.payment_count, 0) as payment_count,
    coalesce(p.payment_method, 'unknown') as payment_method,
    coalesce(p.payment_status, 'unpaid') as payment_status,
    p.paid_at,
    greatest(o.updated_at, coalesce(p.last_payment_updated_at, o.updated_at)) as updated_at
from orders o
left join payments_agg p
    on o.order_id = p.order_id
