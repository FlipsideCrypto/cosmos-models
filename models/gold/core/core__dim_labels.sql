{{ config(
    materialized = 'view'
) }}

SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name,
    COALESCE (
        dim_tokens_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS dim_labels_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('core__dim_tokens') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name,
    COALESCE (
        fact_validators_id,
        {{ dbt_utils.generate_surrogate_key(
            ['address']
        ) }}
    ) AS dim_labels_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('gov__fact_validators') }}
UNION ALL
SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    address_name AS label,
    project_name,
    labels_combined_id AS dim_labels_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__address_labels') }}
WHERE
    label_subtype <> 'validator'
