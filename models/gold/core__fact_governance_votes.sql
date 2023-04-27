{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','proposal_id','voter','vote_option'],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} }
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    voter,
    proposal_id,
    vote_option,
    vote_weight,
    _inserted_timestamp
FROM
    {{ ref('silver__governance_votes') }}

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
