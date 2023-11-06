{% macro cents_to_dollars(col_name) %}
{{col_name}}/100
{% endmacro %}