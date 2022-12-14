{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, proposal_id, voter)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH max_index AS (

    SELECT
        tx_id,
        MAX(msg_index) AS max_idx
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'proposal_vote'
        AND attribute_key = 'option'
    GROUP BY tx_id
),
vote_options AS (
    SELECT
        m.tx_id,
        msg_index,
        CASE
            WHEN attribute_value :: STRING = 'VOTE_OPTION_YES' THEN 1
            WHEN attribute_value :: STRING = 'VOTE_OPTION_ABSTAIN' THEN 2
            WHEN attribute_value :: STRING = 'VOTE_OPTION_NO' THEN 3
            WHEN attribute_value :: STRING = 'VOTE_OPTION_NO_WITH_VETO' THEN 4
            ELSE TRY_PARSE_JSON(attribute_value) :option
        END AS vote_option,
        TRY_PARSE_JSON(attribute_value) :weight :: FLOAT AS vote_weight
    FROM
        {{ ref('silver__msg_attributes') }}
        m
        INNER JOIN max_index x
        ON m.tx_id = x.tx_id
        AND m.msg_index = x.max_idx
    WHERE
        msg_type = 'proposal_vote'
        AND attribute_key = 'option'
        AND vote_option IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= CURRENT_DATE - 2
{% endif %}
),
proposal_id AS (
    SELECT
        tx_id,
        block_id, 
        block_timestamp, 
        tx_succeeded,
        msg_index,
        attribute_value AS proposal_id, 
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'proposal_vote'
        AND attribute_key = 'proposal_id'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= CURRENT_DATE - 2
{% endif %}
),
voter AS (
    SELECT
        tx_id,
        msg_index,
        attribute_value AS voter
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'sender'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= CURRENT_DATE - 2
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    o.tx_id,
    tx_succeeded,
    v.voter,
    p.proposal_id :: NUMBER AS proposal_id,
    vote_option :: NUMBER AS vote_option,
    vote_weight,
    _inserted_timestamp
FROM
    vote_options o
    LEFT OUTER JOIN proposal_id p
    ON o.tx_id = p.tx_id
    AND o.msg_index = p.msg_index
    LEFT OUTER JOIN voter v
    ON o.tx_id = v.tx_id
    AND o.msg_index = v.msg_index - 1
