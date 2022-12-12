{{ config(
    materialized = 'view'
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
    concat_ws(
        '-',
        address,
        creator,
        blockchain
    ) AS _unique_key
FROM
    {{ source(
        'osmo',
        'asset_metadata'
    ) }}
