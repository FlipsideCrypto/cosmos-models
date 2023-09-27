{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_validators(object_construct('sql_source', '{{this.identifier}}','sm_node_path','prod/cosmos/allthatnode/mainnet_ch1/rpc','call_type','non_batch','external_table','validators_ch1'))",
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
EXCEPT
SELECT
    id,
    block_number
FROM
    {{ ref("streamline__complete_validators_ch1") }}
ORDER BY
    block_number
    
