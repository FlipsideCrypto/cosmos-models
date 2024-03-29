{{ config(
  materialized = 'table',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH base_msgs AS (

  SELECT
    t.block_id,
    t.block_timestamp,
    t.tx_id,
    t.gas_used,
    t.gas_wanted,
    t.tx_succeeded,
    f.value AS msg,
    f.index :: INT AS msg_index,
    msg :type :: STRING AS msg_type,
    IFF(
      TRY_BASE64_DECODE_STRING(
        msg :attributes [0] :key :: STRING
      ) = 'action',
      TRUE,
      FALSE
    ) AS is_action,
    NULLIF(
      (conditional_true_event(is_action) over (PARTITION BY tx_id
      ORDER BY
        msg_index ASC) -1),
        -1
    ) AS msg_group,
    IFF(
      TRY_BASE64_DECODE_STRING(
        msg :attributes [0] :key :: STRING
      ) = 'module',
      TRUE,
      FALSE
    ) AS is_module,
    TRY_BASE64_DECODE_STRING(
      msg :attributes [0] :key :: STRING
    ) AS attribute_key,
    TRY_BASE64_DECODE_STRING(
      msg :attributes [0] :value :: STRING
    ) AS attribute_value,
    t._inserted_timestamp
  FROM
    {{ ref(
      'silver__transactions_ch2'
    ) }}
    t,
    LATERAL FLATTEN(
      input => msgs
    ) f
),
exec_actions AS (
  SELECT
    DISTINCT tx_id,
    msg_group
  FROM
    base_msgs
  WHERE
    msg_type = 'message'
    AND attribute_key = 'action'
    AND LOWER(attribute_value) LIKE '%exec%'
),
GROUPING AS (
  SELECT
    base_msgs.tx_id,
    base_msgs.msg_index,
    RANK() over(
      PARTITION BY base_msgs.tx_id,
      base_msgs.msg_group
      ORDER BY
        base_msgs.msg_index
    ) -1 AS msg_sub_group
  FROM
    base_msgs
    INNER JOIN exec_actions e
    ON base_msgs.tx_id = e.tx_id
    AND base_msgs.msg_group = e.msg_group
  WHERE
    base_msgs.is_module = 'TRUE'
    AND base_msgs.msg_type = 'message'
),
msgs AS (
  SELECT
    block_id,
    block_timestamp,
    A.tx_id,
    tx_succeeded,
    msg_group,
    CASE
      WHEN msg_group IS NULL THEN NULL
      ELSE COALESCE(
        LAST_VALUE(
          b.msg_sub_group ignore nulls
        ) over(
          PARTITION BY A.tx_id,
          msg_group
          ORDER BY
            A.msg_index DESC rows unbounded preceding
        ),
        0
      )
    END AS msg_sub_group,
    A.msg_index,
    msg_type,
    msg,
    concat_ws(
      '-',
      A.tx_id,
      A.msg_index
    ) AS unique_key,
    _inserted_timestamp
  FROM
    base_msgs A
    LEFT JOIN GROUPING b
    ON A.tx_id = b.tx_id
    AND A.msg_index = b.msg_index
)
SELECT
  block_id,
  block_timestamp,
  tx_id,
  tx_succeeded,
  msg_group,
  msg_sub_group,
  msg_index,
  msg_type,
  msg :: OBJECT AS msg,
  unique_key,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_id','msg_index']
  ) }} AS msgs_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  _inserted_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  msgs
