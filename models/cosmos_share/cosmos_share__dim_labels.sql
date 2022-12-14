{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', blockchain, address, creator)",
    tags = ['share']
) }}

SELECT
    'cosmos' AS blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name
FROM
    {{ ref('core__dim_labels') }}
