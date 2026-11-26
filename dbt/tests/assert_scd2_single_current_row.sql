-- Singular dbt test: each customer in snap_customers must have exactly one current open record.
select
    customer_id,
    sum(case when dbt_valid_to is null then 1 else 0 end) as current_row_count
from {{ ref('snap_customers') }}
group by customer_id
having sum(case when dbt_valid_to is null then 1 else 0 end) != 1
