{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_payments') }}
),

cleaned as (
    select
        trim(cast(payment_id as text)) as payment_id,
        trim(cast(order_id as text)) as order_id,
        lower(trim(cast(payment_method as text))) as payment_method,
        lower(trim(cast(payment_status as text))) as payment_status,
        cast(coalesce(amount_paid, 0) as numeric(14, 2)) as amount_paid,
        cast(coalesce(gateway_fee, 0) as numeric(14, 2)) as gateway_fee,
        cast(paid_at as timestamp) as paid_at,
        cast(coalesce(updated_at, paid_at) as timestamp) as updated_at
    from source
    where payment_id is not null
)

select * from cleaned
