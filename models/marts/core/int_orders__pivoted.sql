with
    payments as (select * from {{ ref("stg_payments") }}),

    {%- set methods = dbt_utils.get_column_values(table=ref('stg_payments'), column='payment_method') -%}
    
    orders_pivoted as (
        select
            order_id,
            {% for method in methods -%}
                sum(
                    case when payment_method = "{{ method }}" then amount else 0 end
                ) as {{ method }}_payments
                {%- if not loop.last -%}
                ,
                {%- endif %}
            {% endfor %}
        from payments
        group by 1
    )

select *
from
    orders_pivoted

