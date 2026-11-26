{% snapshot snap_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_name', 'category', 'unit_price', 'currency', 'is_active'],
        invalidate_hard_deletes=True
    )
}}

select
    product_id,
    product_name,
    category,
    unit_price,
    currency,
    is_active,
    updated_at
from {{ ref('stg_products') }}

{% endsnapshot %}
