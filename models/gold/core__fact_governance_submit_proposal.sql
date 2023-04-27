{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} }
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    proposer,
    proposal_id,
    proposal_type,
    _inserted_timestamp
FROM
    {{ ref('silver__governance_submit_proposal') }}

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
