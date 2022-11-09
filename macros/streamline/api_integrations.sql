{% macro create_aws_cosmos_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_cosmos_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/snowflake-api-cosmos' api_allowed_prefixes = (
            'https://z97ik1b2d0.execute-api.us-east-1.amazonaws.com/dev/',
            'https://dazi3rled6.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
