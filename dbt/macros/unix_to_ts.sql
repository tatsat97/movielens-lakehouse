{% macro unix_to_ts(column_name) -%}
    to_timestamp(from_unixtime({{ column_name }}))
{%- endmacro %}