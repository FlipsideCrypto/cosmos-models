{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_validators(object_construct('sql_source', '{{this.identifier}}', 'call_type','NON_BATCH', 'batch_call_limit', 20))",
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
    block_number > 13000000
    AND block_number IS NOT NULL
EXCEPT
SELECT
    id,
    block_number
FROM
    {{ ref("streamline__complete_validators") }}
WHERE
    block_number > 13000000
