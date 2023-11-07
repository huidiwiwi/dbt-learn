-- manage access to the datasets: define grants that apply to the models, seed, snapshots to implment these permissions.
-- or use configs in .yml files, see https://docs.getdbt.com/reference/resource-configs/grants

-- !!! this macro is for snowflake users! bigquery usr need configs, cos grant access are not compatible across dataplatforms

{% macro grant_select(schema=target.schema, role=target.role) %}
    {% set query %}
        grant usage on schema {{schema}} to role {{role}};
        grant select on all tables in schema {{schema}} to role {{role}};
        grant select on all views in schema {{schema}} to role {{role}};
    {% endset %}
    {{ log('granting select on all tables and views in schema' ~ schema ~ 'to role' ~ role, info=True)}}
    {% do run_query(query) %}
    {{ log('privileges granted', info=True)}}
{% endmacro %}


