{{ config(
    materialized = 'view'
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
    COALESCE (
        msgs_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','msg_index']
        ) }}
    ) AS fact_msgs_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__msgs') }}
UNION ALL
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
    msgs_id as fact_msgs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__msgs_ch1') }}
UNION ALL
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
    msgs_id as fact_msgs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__msgs_ch2') }}
UNION ALL
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
    msgs_id as fact_msgs_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__msgs_ch3') }}


