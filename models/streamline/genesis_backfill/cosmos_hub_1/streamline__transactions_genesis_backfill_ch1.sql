{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_transactions(object_construct('sql_source', '{{this.identifier}}','sm_node_path','prod/cosmos/allthatnode/mainnet_ch1/rpc','call_type','non_batch'))",
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
    block_number between 110640 and 110650 
    AND block_number IS NOT NULL
EXCEPT
SELECT
    id,
    block_number
FROM
    {{ ref("streamline__complete_transactions_ch1") }}
WHERE
    block_number between 110500 and 110550 
    AND block_number IS NOT NULL
ORDER BY
    block_number
    
