{% macro no_nulls_in_columns(model) %}
    select *
    from {{ model }}
    where
    {% for col in adapter.get_columns_in_relation(model) -%}
        {{ adapter.quote(col.name) }} is null or
    {% endfor %}
    false
{% endmacro %}