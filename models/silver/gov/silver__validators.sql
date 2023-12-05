{{ config(
    materialized = 'table'
) }}

SELECT
    A.address,
    'cosmos' AS blockchain,
    'flipside' AS creator,
    'operator' AS label_type,
    'validator' AS label_subtype,
    A.data :description :moniker :: STRING AS label,
    A.data :description :identity :: STRING AS project_name,
    b.data :acc_address :: STRING AS account_address,
    A.data :delegator_shares :: NUMBER AS delegator_shares,
    b.data :self_stake :: NUMBER AS self_delegation,
    b.data :delegators :: NUMBER num_delegators,
    A.data :commission :commission_rates :rate :: NUMBER AS rate,
    A.data :commission :commission_rates :max_change_rate :: NUMBER AS max_change_rate,
    A.data :commission :commission_rates :max_rate :: NUMBER AS max_rate,
    A.data :commission :update_time :: datetime AS commission_rate_last_updated,
    A.data :status :: STRING AS status,
    A.data :jailed :: BOOLEAN AS jailed,
    RANK() over (
        ORDER BY
            A.data :delegator_shares :: NUMBER DESC
    ) AS RANK,
    b.data :governance_votes :: NUMBER AS num_governance_votes,
    A.data AS raw_metadata,
    concat_ws(
        '-',
        A.address,
        creator,
        blockchain
    ) AS unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['unique_key']
    ) }} AS validators_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze_api__get_validator_metadata_lcd') }} A
    LEFT JOIN bronze_api.get_validator_metadata b
    ON A.address = b.address
