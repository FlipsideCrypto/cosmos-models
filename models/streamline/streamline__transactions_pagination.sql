{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_transactions(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['block_number']
    ) }} AS id,
    block_number
FROM
    {{ source(
        "bronze_streamline",
        "tx_pagination_blocks"
    ) }}
LIMIT
    4000
