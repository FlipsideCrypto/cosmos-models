{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH

{% if is_incremental() %}
max_date AS (

    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
),
{% endif %}

proposal_ids AS (
    SELECT
        tx_id,
        block_id, 
        block_timestamp, 
        tx_succeeded,
        attribute_value AS proposal_id, 
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'proposal_deposit'
        AND attribute_key = 'proposal_id'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
deposit_value AS (
    SELECT
        tx_id,
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM
        {{ ref('silver__msg_attributes') }}
        m
    WHERE
        msg_type = 'proposal_deposit'
        AND attribute_key = 'amount'
        AND attribute_value IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        max_date
)
{% endif %}
),
depositors AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS depositor
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    p.tx_id,
    tx_succeeded,
    d.depositor,
    p.proposal_id :: NUMBER as proposal_id,
    v.amount :: NUMBER as amount,
    v.currency,
    _inserted_timestamp
FROM
    deposit_value v
    INNER JOIN proposal_ids p
    ON p.tx_id = v.tx_id
    INNER JOIN depositors d
    ON v.tx_id = d.tx_id
