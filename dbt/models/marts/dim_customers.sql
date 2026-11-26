{{ config(materialized='table') }}

with current_customers as (
    select
        md5(customer_id || '|' || cast(dbt_valid_from as text)) as customer_sk,
        customer_id,
        customer_name,
        email,
        segment,
        plan_tier,
        status,
        country,
        created_at,
        updated_at,
        dbt_valid_from,
        dbt_valid_to,
        (dbt_valid_to is null) as is_current
    from {{ ref('snap_customers') }}
    where dbt_valid_to is null
)

select * from current_customers
