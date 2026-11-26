{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

cleaned as (
    select
        trim(cast(order_id as text)) as order_id,
        trim(cast(customer_id as text)) as customer_id,
        trim(cast(product_id as text)) as product_id,
        cast(order_date as date) as order_date,
        coalesce(cast(quantity as integer), 1) as quantity,
        cast(coalesce(gross_amount, 0) as numeric(14, 2)) as gross_amount,
        cast(coalesce(discount_amount, 0) as numeric(14, 2)) as discount_amount,
        cast(coalesce(tax_amount, 0) as numeric(14, 2)) as tax_amount,
        lower(trim(cast(order_status as text))) as order_status,
        upper(trim(coalesce(cast(currency as text), 'USD'))) as currency,
        cast(coalesce(updated_at, cast(order_date as timestamp)) as timestamp) as updated_at
    from source
    where order_id is not null
)

select * from cleaned
