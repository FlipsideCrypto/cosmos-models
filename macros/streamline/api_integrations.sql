{% macro create_aws_cosmos_api() %}
    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_cosmos_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/cosmos-api-prod-rolesnowflakeudfsAF733095-JUPRO23RUCFW' api_allowed_prefixes = (
            'https://7kal4st06d.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_cosmos_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/cosmos-api-dev-rolesnowflakeudfsAF733095-1SQ9TX1BQRUE' api_allowed_prefixes = (
            'https://qkwbozz9l0.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
        {% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
