{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_count,
    proposer_address,
    validator_hash
FROM
    {{ ref('core__fact_blocks') }}
