{{ config(
    materialized = 'view'
) }}

SELECT
    address,
    blockchain,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    account_address,
    delegator_shares,
    self_delegation,
    num_delegators,
    rate,
    max_change_rate,
    max_rate,
    commission_rate_last_updated,
    status,
    jailed,
    RANK,
    num_governance_votes,
    raw_metadata,
    unique_key,
    COALESCE (
        validators_id,
        {{ dbt_utils.generate_surrogate_key(
            ['unique_key']
        ) }}
    ) AS fact_validators_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__validators') }}
