{% macro create_udfs() %}
    {% set sql %}
    CREATE schema if NOT EXISTS silver;
    {{ create_udtf_get_base_table(
        schema = "streamline"
    ) }}

    {% endset %}
    {% do run_query(sql) %}
        {% set sql %}
        {# {{ create_udf_get_cosmos_chainhead() }} #}
        {{ create_udf_get_cosmos_blocks() }}
        {% endset %}
        {% do run_query(sql) %}
{% endmacro %}