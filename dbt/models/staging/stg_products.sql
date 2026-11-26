{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_products') }}
),

cleaned as (
    select
        trim(cast(product_id as text)) as product_id,
        trim(cast(product_name as text)) as product_name,
        trim(cast(category as text)) as category,
        cast(unit_price as numeric(12, 2)) as unit_price,
        upper(trim(coalesce(cast(currency as text), 'USD'))) as currency,
        cast(coalesce(is_active, true) as boolean) as is_active,
        cast(updated_at as timestamp) as updated_at
    from source
    where product_id is not null
)

select * from cleaned
