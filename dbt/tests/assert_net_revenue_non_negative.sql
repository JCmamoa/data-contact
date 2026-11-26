-- Singular dbt test: net_revenue must be non-negative and match the canonical formula.
select
    order_id,
    gross_amount,
    discount_amount,
    tax_amount,
    gateway_fee,
    net_revenue
from {{ ref('mart_revenue_analytics') }}
where net_revenue < 0
   or abs(
       net_revenue - (
           coalesce(gross_amount, 0)
           - coalesce(discount_amount, 0)
           - coalesce(tax_amount, 0)
           - coalesce(gateway_fee, 0)
       )
   ) >= 0.01
