{%- macro limit_data_in_dev(timestamp_col, dev_days_of_date=3) -%}
{%- if target.name == "dev" %}
where {{timestamp_col}} >= {{- dateadd('day',-dev_days_of_date, current_timestamp() ) -}}
{%- endif -%}
{%- endmacro -%}