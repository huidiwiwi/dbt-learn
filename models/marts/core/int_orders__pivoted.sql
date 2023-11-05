with
    payments as (select * from {{ ref("stg_payments") }}),

    {%- set methods = dbt_utils.get_column_values(table=ref('stg_payments'), column='payment_method') -%}

    orders_pivoted as (
        select
            order_id,
            {{ dbt_utils.pivot(column="payment_method", agg='sum', values=methods, then_value="amount", suffix='_payments') }}
        from payments
        group by 1
    )

select *
from
    orders_pivoted

