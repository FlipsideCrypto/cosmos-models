{{ config(
    materialized = 'view'
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
    COALESCE (
        transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['unique_key']
        ) }}
    ) AS fact_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__transfers') }}
