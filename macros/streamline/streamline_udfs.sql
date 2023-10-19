{% macro create_udf_get_cosmos_blocks() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_cosmos_blocks(
        json variant
    ) returns text api_integration = aws_cosmos_api AS {% if target.name == "prod" %}
        'https://dazi3rled6.execute-api.us-east-1.amazonaws.com/prod/bulk_get_cosmos_blocks'
    {% else %}
        'https://qkwbozz9l0.execute-api.us-east-1.amazonaws.com/dev/bulk_get_cosmos_blocks'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_cosmos_transactions() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_cosmos_transactions(
        json variant
    ) returns text api_integration = aws_cosmos_api AS {% if target.name == "prod" %}
        'https://dazi3rled6.execute-api.us-east-1.amazonaws.com/prod/bulk_get_cosmos_transactions'
    {% else %}
        'https://qkwbozz9l0.execute-api.us-east-1.amazonaws.com/dev/bulk_get_cosmos_transactions'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_cosmos_validators() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_cosmos_validators(
        json variant
    ) returns text api_integration = aws_cosmos_api AS {% if target.name == "prod" %}
        'https://dazi3rled6.execute-api.us-east-1.amazonaws.com/prod/bulk_get_cosmos_validators'
    {% else %}
        'https://qkwbozz9l0.execute-api.us-east-1.amazonaws.com/dev/bulk_get_cosmos_validators'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_cosmos_generic() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_get_cosmos_generic(
        json variant
    ) returns text api_integration = aws_cosmos_api AS {% if target.name == "prod" %}
        'https://dazi3rled6.execute-api.us-east-1.amazonaws.com/prod/bulk_get_cosmos_generic'
    {% else %}
        'https://qkwbozz9l0.execute-api.us-east-1.amazonaws.com/dev/bulk_get_cosmos_generic'
    {%- endif %};
{% endmacro %}

{% macro create_udf_get_cosmos_chainhead() %}
    CREATE EXTERNAL FUNCTION IF NOT EXISTS streamline.udf_get_cosmos_chainhead() returns variant api_integration = aws_cosmos_api AS {% if target.name == "prod" %}
        'https://dazi3rled6.execute-api.us-east-1.amazonaws.com/prod/get_cosmos_chainhead'
    {% else %}
        'https://qkwbozz9l0.execute-api.us-east-1.amazonaws.com/dev/get_cosmos_chainhead'
    {%- endif %};
{% endmacro %}
