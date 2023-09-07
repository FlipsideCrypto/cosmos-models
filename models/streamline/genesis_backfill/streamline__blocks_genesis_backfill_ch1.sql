{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_blocks(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS id,
    block_number
FROM
    {{ ref("streamline__blocks_ch1") }}
WHERE
    block_number between 0 and 10
    AND block_number IS NOT NULL

