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
    RANK,
    num_governance_votes,
    raw_metadata,
    unique_key
FROM
    {{ ref('silver__validators') }}
