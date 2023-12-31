{{ config(
    materialized = 'table',
    cluster_by = ['block_timestamp::DATE'],
    enabled = false
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    msg_group,
    msg_sub_group,
    msg_index,
    msg_type,
    0 AS attribute_index,
    TRY_BASE64_DECODE_STRING(
        msg :key :: STRING
    ) AS attribute_key,
    TRY_BASE64_DECODE_STRING(
        msg :value :: STRING
    ) AS attribute_value,
    concat_ws(
        '-',
        tx_id,
        msg_index,
        attribute_index
    ) AS unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','msg_index','attribute_index']
    ) }} AS msg_attributes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__msgs_ch1') }} A
