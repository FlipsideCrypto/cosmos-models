{{ config(
  materialized = 'incremental',
  unique_key = "unique_key",
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
    {{ ref('silver__transactions') }}
),
{% endif %}

fee AS (
  SELECT
    block_id, 
    tx_id,
    attribute_value AS fee
  FROM
    {{ ref('silver__msg_attributes') }}
  WHERE
    attribute_key = 'fee'
    AND msg_type = 'tx'
    
{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
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
    {{ ref('silver__msg_attributes') }}
  WHERE
    attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
  msg_index)) = 1
),

no_fee_tx_raw as (
 select  
    block_id, 
    tx_id, 
    f.index,
    f.value: type ::string as event_type,
    try_base64_decode_string( f.value:attributes[0]:value) as recipient,
    try_base64_decode_string( f.value:attributes[1]:value) as sender ,
    try_base64_decode_string( f.value:attributes[2]:value) as amount_raw ,
    CASE 
      WHEN amount_raw like '%uatom' AND amount_raw not like '%ibc%' 
        THEN amount_raw 
      ELSE '0uatom' 
    END as amount 

    FROM
        {{ ref('silver__transactions') }} , 
        TABLE (flatten (input => msgs)) f
          
    WHERE 
        tx_id NOT IN 
            (select 
                tx_id 
                from fee
                )
     AND event_type = 'transfer'
    
     qualify row_number() over (partition by tx_id order by f.index asc) = 1 
    ),
    
no_fee_transactions AS (
SELECT
  t.block_id,
  t.block_timestamp,
  t.tx_id,
  f.sender as tx_from,
  tx_succeeded,
  codespace,
  COALESCE(
    amount,
    '0uatom'
  ) AS fee_raw,
  regexp_substr(fee_raw, '[0-9]+') as fee, 
  regexp_substr(fee_raw, '[a-z]+') as fee_denom, 
  gas_used,
  gas_wanted,
  tx_code,
  tx_log,
  msgs,
  _inserted_timestamp, 
  unique_key
FROM
  {{ ref('silver__transactions') }} 
  t
  INNER JOIN no_fee_tx_raw f
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
  regexp_substr(fee_raw, '[0-9]+') as fee, 
  regexp_substr(fee_raw, '[a-z]+') as fee_denom, 
  gas_used,
  gas_wanted,
  tx_code,
  tx_log,
  msgs,
  _inserted_timestamp, 
  unique_key
FROM
  {{ ref('silver__transactions') }} 
  t
  INNER JOIN fee f
  ON t.tx_id = f.tx_id
  AND t.block_id = f.block_id
  INNER JOIN spender s
  ON t.tx_id = s.tx_id
  AND t.block_id = s.block_id
), 

final_transactions as (
select * 
    from no_fee_transactions

union all 

select * 
    from fee_transactions
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

FROM final_transactions