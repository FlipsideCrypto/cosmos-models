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
    CASE
        WHEN block_id >= 19639600
        OR (
            block_id BETWEEN 19639060
            AND 19639600
            AND TRY_BASE64_DECODE_STRING(
                b.value :key
            ) IS NULL
        ) THEN b.value :key
        ELSE TRY_BASE64_DECODE_STRING(
            b.value :key
        )
    END AS attribute_key,
    CASE
        WHEN block_id >= 19639600
        OR (
            block_id BETWEEN 19639060
            AND 19639600
            AND TRY_BASE64_DECODE_STRING(
                b.value :key
            ) IS NULL
        ) THEN b.value :value
        ELSE TRY_BASE64_DECODE_STRING(
            b.value :value
        )
    END AS attribute_value,
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
    {{ ref('silver__msgs') }} A,
    LATERAL FLATTEN(
        input => A.msg,
        path => 'attributes'
    ) b

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
