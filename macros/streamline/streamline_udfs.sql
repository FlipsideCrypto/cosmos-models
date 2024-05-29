{% macro create_udf_bulk_rest_api_v2() %}
    CREATE
    OR REPLACE EXTERNAL FUNCTION streamline.udf_bulk_rest_api_v2(
        json OBJECT
    ) returns ARRAY api_integration = {% if target.name == "prod" %}
        aws_cosmos_api_prod AS 'https://kpg3w2qkm4.execute-api.us-east-1.amazonaws.com/prod/udf_bulk_rest_api'
    {% else %}
        aws_cosmos_api_stg AS 'https://e8nbzsw4r9.execute-api.us-east-1.amazonaws.com/stg/udf_bulk_rest_api'
    {%- endif %};
{% endmacro %}
