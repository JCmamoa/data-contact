{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

cleaned as (
    select
        trim(cast(customer_id as text)) as customer_id,
        trim(cast(customer_name as text)) as customer_name,
        lower(trim(cast(email as text))) as email,
        trim(cast(segment as text)) as segment,
        trim(cast(plan_tier as text)) as plan_tier,
        lower(trim(cast(status as text))) as status,
        upper(trim(cast(country as text))) as country,
        cast(created_at as timestamp) as created_at,
        cast(coalesce(updated_at, created_at) as timestamp) as updated_at
    from source
    where customer_id is not null
)

select * from cleaned
