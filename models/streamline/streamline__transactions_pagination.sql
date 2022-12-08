{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_transactions(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    block_number
FROM
    {{ source(
        "bronze_streamline",
        "tx_search"
    ) }}
WHERE
    DATA :result :total_count :: INT > 100
LIMIT
    5000
