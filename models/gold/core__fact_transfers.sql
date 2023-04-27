{{ config(
    materialized = 'incremental',
    unique_key = "unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    transfer_type,
    sender,
    amount,
    currency,
    receiver,
    unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__transfers') }}
