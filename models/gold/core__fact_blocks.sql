{{ config(
    materialized = 'incremental',
    unique_key = ["block_id","chain_id"],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT
    block_id,
    block_timestamp,
    blockchain,
    chain_id,
    tx_count,
    proposer_address,
    validator_hash,
    _inserted_timestamp
FROM
    {{ ref('silver__blocks') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
