{{ config(
    materialized = 'incremental',
    unique_key = "unique_key",
    tags = ['share']
) }}

SELECT
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    label,
    project_name,
    alias,
    DECIMAL,
    raw_metadata,
    unique_key
FROM
    {{ ref('core__dim_tokens') }}
