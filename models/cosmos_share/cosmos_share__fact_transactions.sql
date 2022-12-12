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
    tx_from,
    tx_succeeded,
    codespace,
    fee,
    fee_denom,
    gas_used,
    gas_wanted,
    tx_code,
    tx_log,
    msgs,
    _partition_by_block_id,
    unique_key
FROM
    {{ ref('core__fact_transactions') }}
