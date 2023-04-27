{{ config(
    materialized = 'incremental',
    unique_key = 'unique_key',
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_succeeded,
    CONCAT(
        msg_group,
        ':',
        msg_sub_group
    ) AS msg_group,
    msg_index,
    msg_type,
    msg,
    unique_key,
    _inserted_timestamp
FROM
    {{ ref('silver__msgs') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}
