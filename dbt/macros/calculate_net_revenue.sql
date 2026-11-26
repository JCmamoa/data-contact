{% macro calculate_net_revenue(
    gross_col='gross_amount',
    discount_col='discount_amount',
    tax_col='tax_amount',
    fee_col='gateway_fee'
) %}
    round(
        (
            coalesce({{ gross_col }}, 0)
            - coalesce({{ discount_col }}, 0)
            - coalesce({{ tax_col }}, 0)
            - coalesce({{ fee_col }}, 0)
        )::numeric,
        2
    )
{% endmacro %}
