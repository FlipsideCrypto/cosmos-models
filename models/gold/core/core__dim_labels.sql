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
    project_name
FROM
    {{ ref('core__dim_tokens') }}
UNION
SELECT
    blockchain,
    creator,
    address,
    label_type,
    label_subtype,
    label,
    project_name
FROM
    {{ ref('core__fact_validators') }}
