{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_transactions(object_construct('sql_source', '{{this.identifier}}','sm_node_path','prod/cosmos/allthatnode/mainnet_ch2/rpc','call_type','non_batch','external_table','tx_search_ch2','producer_batch_size','56000','worker_batch_size','800'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS id,
    block_number
FROM
    {{ ref("streamline__blocks_ch2") }}
EXCEPT
SELECT
    id,
    block_number
FROM
    {{ ref("streamline__complete_transactions_ch2") }}
ORDER BY
    block_number
LIMIT
    56000
