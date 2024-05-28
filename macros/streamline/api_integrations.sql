{% macro create_aws_cosmos_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_cosmos_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/cosmos-api-prod-rolesnowflakeudfsAF733095-14KYNLUQ3CWV2' api_allowed_prefixes = (
            'https://bp6s0ib6fk.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_cosmos_api_stg api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::704693948482:role/cosmos-api-stg-rolesnowflakeudfsAF733095-MWKNVHtNSA9n' api_allowed_prefixes = (
            'https://e8nbzsw4r9.execute-api.us-east-1.amazonaws.com/stg/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
