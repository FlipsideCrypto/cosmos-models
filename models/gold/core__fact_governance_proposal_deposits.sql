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
    depositor,
    proposal_id,
    amount / pow(10, COALESCE(t.decimal, 0)) :: NUMBER AS amount,
    currency,
    t.decimal,
    _inserted_timestamp
FROM
    {{ ref('silver__governance_proposal_deposits') }}
    g
    LEFT OUTER JOIN {{ ref('core__dim_tokens') }}
    t
    ON g.currency = t.address

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
