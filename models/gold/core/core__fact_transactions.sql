{{ config(
  materialized = 'view'
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
  _inserted_timestamp,
  unique_key,
  COALESCE (
    transactions_final_id,
    {{ dbt_utils.generate_surrogate_key(
      ['tx_id']
    ) }}
  ) AS fact_transactions_id,
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  {{ ref('silver__transactions_final') }}
