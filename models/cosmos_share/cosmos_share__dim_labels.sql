{{ config(
    materialized = 'incremental',
    unique_key = "unique_key",
    tags = ['share']
) }}

SELECT
    'cosmos' AS blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name, 
    concat_ws(
        '-',
        blockchain,
        address,
        creator
    ) AS unique_key
FROM
    {{ ref('core__dim_labels') }}
