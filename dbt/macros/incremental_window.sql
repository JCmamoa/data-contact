{% macro incremental_window(watermark_col='updated_at', lookback_days=0) %}
    {% if is_incremental() %}
        where {{ watermark_col }} >= (
            select coalesce(
                max({{ watermark_col }})
                {% if lookback_days > 0 %}
                    - interval '{{ lookback_days }} days'
                {% endif %},
                '1970-01-01'::timestamp
            )
            from {{ this }}
        )
    {% endif %}
{% endmacro %}
