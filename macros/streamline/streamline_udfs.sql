{% macro create_udf_get_cosmos_blocks() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_cosmos_blocks(
        json variant
    ) returns text api_integration = aws_cosmos_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/bulk_get_cosmos_blocks'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/bulk_get_cosmos_blocks'
    {%- endif %};
{% endmacro %}

{# {% macro create_udf_get_cosmos_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_chainhead() returns variant api_integration = aws_cosmos_api AS {% if target.name == "prod" %}
        'https://e03pt6v501.execute-api.us-east-1.amazonaws.com/prod/get_cosmos_chainhead'
    {% else %}
        'https://mryeusnrob.execute-api.us-east-1.amazonaws.com/dev/get_cosmos_chainhead'
    {%- endif %};
{% endmacro %} #}
