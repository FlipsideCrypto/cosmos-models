{{ config(
    materialized = 'incremental',
    unique_key = ['tx_id','proposal_id','voter','vote_option'],
    incremental_strategy = 'merge',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH base_atts AS (

    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        COALESCE(
            TRY_CAST(
                SPLIT_PART(REPLACE(REPLACE(b.path, '['), ']'), '.', 1) AS INT
            ),
            0
        ) AS msg_sub_sub_group,
        msg_index,
        COALESCE(
            b.key,
            attribute_key
        ) AS attribute_key,
        COALESCE(
            b.value,
            attribute_value
        ) AS attribute_value,
        msg_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__msg_attributes') }},
        LATERAL FLATTEN(
            TRY_PARSE_JSON(
                CASE
                    WHEN attribute_key = 'option'
                    AND attribute_value LIKE '%option%option%' THEN '[' || REGEXP_REPLACE(
                        attribute_value,
                        '\}\n',
                        '\},'
                    ) || ']'
                    ELSE attribute_value
                END
            ),
            outer => TRUE
        ) b
    WHERE
        (
            msg_type = 'proposal_vote'
            OR (
                msg_type = 'message'
                AND attribute_key = 'sender'
            )
        )
        AND COALESCE(
            b.value,
            ''
        ) NOT LIKE '%option%weight%'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) _inserted_timestamp
    FROM
        {{ this }}
)
{% endif %}
),
fin AS (
    SELECT
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        msg_sub_sub_group,
        _inserted_timestamp,
        OBJECT_AGG(
            attribute_key :: STRING,
            attribute_value :: variant
        ) AS j,
        {# j :proposal_id :: INT AS proposal_id, #}
        j :option :: STRING AS vote_option,
        j :weight :: FLOAT AS vote_weight
    FROM
        base_atts
    WHERE
        msg_type = 'proposal_vote'
        AND attribute_key IN (
            'option',
            'weight'
        )
    GROUP BY
        block_id,
        block_timestamp,
        tx_succeeded,
        tx_id,
        msg_group,
        msg_sub_group,
        msg_sub_sub_group,
        _inserted_timestamp
)
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    A.tx_succeeded,
    b.voter,
    C.proposal_id,
    CASE
        WHEN A.vote_option = 'VOTE_OPTION_YES' THEN 1
        WHEN A.vote_option = 'VOTE_OPTION_ABSTAIN' THEN 2
        WHEN A.vote_option = 'VOTE_OPTION_NO' THEN 3
        WHEN A.vote_option = 'VOTE_OPTION_NO_WITH_VETO' THEN 4
        ELSE A.vote_option :: INT
    END AS vote_option,
    A.vote_weight,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_id','c.proposal_id','b.voter','a.vote_option']
    ) }} AS governance_votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    A._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin A
    JOIN (
        SELECT
            tx_id,
            msg_group,
            msg_sub_group,
            attribute_value AS voter
        FROM
            base_atts
        WHERE
            msg_type = 'message'
            AND attribute_key = 'sender'
    ) b
    ON A.tx_id = b.tx_id
    AND A.msg_group = b.msg_group
    AND A.msg_sub_group = b.msg_sub_group
    JOIN (
        SELECT
            tx_id,
            msg_group,
            msg_sub_group,
            attribute_value :: INT AS proposal_id
        FROM
            base_atts
        WHERE
            msg_type = 'proposal_vote'
            AND attribute_key = 'proposal_id'
    ) C
    ON A.tx_id = C.tx_id
    AND A.msg_group = C.msg_group
    AND A.msg_sub_group = C.msg_sub_group qualify(ROW_NUMBER() over (PARTITION BY A.tx_id, b.voter, C.proposal_id, vote_option
ORDER BY
    A.msg_group DESC) = 1)
