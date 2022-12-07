{{ config(
    materialized = 'incremental',
    unique_key = "unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
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
    b.index AS attribute_index,
    TRY_BASE64_DECODE_STRING(
        b.value :key :: STRING
    ) AS attribute_key,
    TRY_BASE64_DECODE_STRING(
        b.value :value :: STRING
    ) AS attribute_value,
    concat_ws(
        '-',
        block_id,
        tx_id,
        msg_index,
        attribute_index
    ) AS unique_key,
    _partition_by_block_id
FROM
    {{ ref('silver__msgs') }} A,
    LATERAL FLATTEN(
        input => A.msg,
        path => 'attributes'
    ) b

{% if is_incremental() %}
WHERE
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id)
        FROM
            {{ this }}
    )
{% endif %}
