{{ config(materialized='table') }}

with current_products as (
    select
        md5(product_id || '|' || cast(dbt_valid_from as text)) as product_sk,
        product_id,
        product_name,
        category,
        unit_price,
        currency,
        is_active,
        updated_at,
        dbt_valid_from,
        dbt_valid_to,
        (dbt_valid_to is null) as is_current
    from {{ ref('snap_products') }}
    where dbt_valid_to is null
)

select * from current_products
