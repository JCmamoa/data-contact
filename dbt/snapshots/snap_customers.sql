{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['plan_tier', 'status', 'segment', 'country', 'customer_name', 'email'],
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    customer_name,
    email,
    segment,
    plan_tier,
    status,
    country,
    created_at,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
