{{ config(
    materialized = 'table'
) }}

SELECT
    'cosmos' AS blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    alias,
    DECIMAL,
    raw_metadata,
    unique_key,
    COALESCE (
        osmo_assets_id,
        {{ dbt_utils.generate_surrogate_key(
            ['unique_key']
        ) }}
    ) AS dim_tokens_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref(
        'silver__osmos_assets'
    ) }}
