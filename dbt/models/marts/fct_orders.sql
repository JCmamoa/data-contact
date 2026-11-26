{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with order_payments as (
    select * from {{ ref('int_order_payments') }}
    {{ incremental_window('updated_at', var('incremental_lookback_days', 0)) }}
),

customers as (
    select customer_id, customer_sk from {{ ref('dim_customers') }}
),

products as (
    select product_id, product_sk from {{ ref('dim_products') }}
)

select
    op.order_id,
    c.customer_sk,
    op.customer_id,
    p.product_sk,
    op.product_id,
    op.order_date,
    op.quantity,
    op.gross_amount,
    op.discount_amount,
    op.tax_amount,
    op.gateway_fee,
    op.amount_paid,
    {{ calculate_net_revenue('op.gross_amount', 'op.discount_amount', 'op.tax_amount', 'op.gateway_fee') }} as net_revenue,
    op.order_status,
    op.currency,
    op.payment_count,
    op.payment_method,
    op.payment_status,
    op.paid_at,
    op.updated_at
from order_payments op
left join customers c
    on op.customer_id = c.customer_id
left join products p
    on op.product_id = p.product_id
