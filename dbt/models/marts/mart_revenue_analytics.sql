{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with orders as (
    select * from {{ ref('fct_orders') }}
    {{ incremental_window('updated_at', var('incremental_lookback_days', 0)) }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
)

select
    o.order_id,
    o.order_date,
    date_trunc('month', o.order_date)::date as order_month,
    o.order_status,
    o.currency,
    o.customer_sk,
    o.customer_id,
    c.customer_name,
    c.email as customer_email,
    c.segment as customer_segment,
    c.plan_tier,
    c.status as customer_status,
    c.country as country,
    c.country as customer_country,
    o.product_sk,
    o.product_id,
    p.product_name,
    p.category as category,
    p.category as product_category,
    p.unit_price,
    o.quantity,
    o.gross_amount,
    o.gross_amount as gross_revenue,
    o.discount_amount,
    o.tax_amount,
    o.gateway_fee,
    o.amount_paid,
    {{ calculate_net_revenue('o.gross_amount', 'o.discount_amount', 'o.tax_amount', 'o.gateway_fee') }} as net_revenue,
    o.payment_method,
    o.payment_status,
    o.paid_at,
    o.updated_at
from orders o
left join customers c
    on o.customer_id = c.customer_id
left join products p
    on o.product_id = p.product_id
