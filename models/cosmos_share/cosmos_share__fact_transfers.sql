{{ config(
    materialized = 'incremental',
    unique_key = "unique_key",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
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
    unique_key
FROM
    {{ ref('core__fact_transfers') }}
