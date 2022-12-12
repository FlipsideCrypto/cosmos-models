{{ config(
    materialized = 'table'
) }}

SELECT
    address,
    'cosmos' AS blockchain,
    'flipside' AS creator,
    'operator' AS label_type,
    'validator' AS label_subtype,
    DATA :title :: STRING AS label,
    DATA :cons_address :: STRING AS project_name,
    DATA :acc_address :: STRING AS account_address,
    DATA :power :: NUMBER AS delegator_shares,
    DATA :self_stake :: NUMBER AS self_delegation,
    DATA :delegators :: NUMBER AS num_delegators,
    DATA :fee :: NUMBER AS rate,
    RANK() over (
        PARTITION BY address
        ORDER BY
            DATA :self_stake DESC
    ) AS RANK,
    DATA :governance_votes :: NUMBER AS num_governance_votes,
    DATA AS raw_metadata,
    concat_ws(
        '-',
        address,
        creator,
        blockchain
    ) AS unique_key
FROM
    {{ ref('bronze_api__get_validator_metadata') }}
