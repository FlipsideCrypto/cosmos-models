{{ config(
    materialized = 'table'
) }}

SELECT
    address,
    'cosmos' AS blockchain,
    'flipside' AS creator,
    'operator' AS label_type,
    'validator' AS label_subtype,
    DATA :description :moniker :: STRING AS label,
    DATA :description :identity :: STRING AS project_name,
    NULL AS account_address,
    DATA :delegator_shares :: NUMBER AS delegator_shares,
    NULL AS self_delegation,
    NULL num_delegators,
    DATA :commission :commission_rates :rate :: NUMBER AS rate,
    DATA :commission :commission_rates :max_change_rate :: NUMBER AS max_change_rate,
    DATA :commission :commission_rates :max_rate :: NUMBER AS max_rate,
    DATA :commission :update_time :: datetime AS commission_rate_last_updated,
    DATA :status :: STRING AS status,
    DATA :jailed :: BOOLEAN AS jailed,
    RANK() over (
        ORDER BY
            DATA :delegator_shares :: NUMBER DESC
    ) AS RANK,
    NULL AS num_governance_votes,
    DATA AS raw_metadata,
    concat_ws(
        '-',
        address,
        creator,
        blockchain
    ) AS unique_key
FROM
    {{ ref('bronze_api__get_validator_metadata') }}
