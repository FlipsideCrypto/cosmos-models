{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH

{% if is_incremental() %}
max_block_partition AS (

  SELECT
    MAX(
      _inserted_timestamp
    ) AS _inserted_timestamp
  FROM
    {{ this }}
),
{% endif %}

atts AS (
  SELECT
    block_id,
    tx_id,
    msg_index,
    msg_type,
    attribute_key,
    attribute_value
  FROM
    {{ ref('silver__msg_attributes') }}
  WHERE
    msg_type = 'tx'
    AND attribute_key IN ('fee', 'acc_seq')

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
txs AS (
  SELECT
    *
  FROM
    {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp :: DATE >= (
    SELECT
      MAX(_inserted_timestamp) :: DATE - 2
    FROM
      {{ this }}
  )
{% endif %}
),
fee AS (
  SELECT
    block_id,
    tx_id,
    attribute_value AS fee
  FROM
    atts
  WHERE
    attribute_key = 'fee'
    AND msg_type = 'tx'
),
spender AS (
  SELECT
    block_id,
    tx_id,
    SPLIT_PART(
      attribute_value,
      '/',
      0
    ) AS tx_from
  FROM
    atts
  WHERE
    attribute_key = 'acc_seq' qualify(ROW_NUMBER() over(PARTITION BY tx_id
  ORDER BY
    msg_index)) = 1
),
no_fee_tx_raw AS (
  SELECT
    block_id,
    tx_id,
    f.index,
    f.value: TYPE :: STRING AS event_type,
    TRY_BASE64_DECODE_STRING(
      f.value :attributes [0] :value
    ) AS recipient,
    TRY_BASE64_DECODE_STRING(
      f.value :attributes [1] :value
    ) AS sender,
    TRY_BASE64_DECODE_STRING(
      f.value :attributes [2] :value
    ) AS amount_raw,
    CASE
      WHEN amount_raw LIKE '%uatom'
      AND amount_raw NOT LIKE '%ibc%' THEN amount_raw
      ELSE '0uatom'
    END AS amount
  FROM
    txs A,
    TABLE (FLATTEN (input => msgs, outer => TRUE)) f
  WHERE
    tx_id NOT IN (
      SELECT
        tx_id
      FROM
        fee
    )
    AND event_type = 'transfer' qualify ROW_NUMBER() over (
      PARTITION BY tx_id
      ORDER BY
        f.index ASC
    ) = 1
),
no_fee_transactions AS (
  SELECT
    t.block_id,
    t.block_timestamp,
    t.tx_id,
    f.sender AS tx_from,
    tx_succeeded,
    codespace,
    COALESCE(
      amount,
      '0uatom'
    ) AS fee_raw,
    REGEXP_SUBSTR(
      fee_raw,
      '[0-9]+'
    ) AS fee,
    REGEXP_SUBSTR(
      fee_raw,
      '[a-z]+'
    ) AS fee_denom,
    gas_used,
    gas_wanted,
    tx_code,
    tx_log,
    msgs,
    _inserted_timestamp,
    unique_key
  FROM
    txs t
    JOIN no_fee_tx_raw f
    ON t.tx_id = f.tx_id
    AND t.block_id = f.block_id
),
fee_transactions AS (
  SELECT
    t.block_id,
    t.block_timestamp,
    t.tx_id,
    s.tx_from,
    tx_succeeded,
    codespace,
    COALESCE(
      fee,
      '0uatom'
    ) AS fee_raw,
    REGEXP_SUBSTR(
      fee_raw,
      '[0-9]+'
    ) AS fee,
    REGEXP_SUBSTR(
      fee_raw,
      '[a-z]+'
    ) AS fee_denom,
    gas_used,
    gas_wanted,
    tx_code,
    tx_log,
    msgs,
    _inserted_timestamp,
    unique_key
  FROM
    txs t
    INNER JOIN fee f
    ON t.tx_id = f.tx_id
    AND t.block_id = f.block_id
    INNER JOIN spender s
    ON t.tx_id = s.tx_id
    AND t.block_id = s.block_id
),
final_transactions AS (
  SELECT
    *
  FROM
    no_fee_transactions
  UNION ALL
  SELECT
    *
  FROM
    fee_transactions
)
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
  unique_key
FROM
  final_transactions
