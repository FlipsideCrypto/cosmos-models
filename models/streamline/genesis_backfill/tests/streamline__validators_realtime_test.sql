{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_validators(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS id,
    block_number
FROM
    {{ ref("streamline__blocks") }}
WHERE
    block_number between 17204185 and 17204388

