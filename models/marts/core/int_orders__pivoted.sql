with
    payments as (select * from {{ ref("stg_payments") }}),

    {%- set methods_query -%}
        select distinct payment_method
        from {{ ref("stg_payments") }}
    {%- endset -%}
    
    {%- set results = run_query(methods_query) -%}

    {%- if execute -%}
    {%- set methods = results.columns[0].values() -%}
    {%- else -%}
    {%- set methods = [] -%}
    {%- endif -%}

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

