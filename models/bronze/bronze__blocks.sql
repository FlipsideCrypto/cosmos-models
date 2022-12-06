{{ config(
    materialized = 'view'
) }}

SELECT
    VALUE,
    _partition_by_block_id,
    block_number AS block_id,
    metadata,
    DATA
FROM
    {{ source(
        'bronze_streamline',
        'blocks'
    ) }}
WHERE  
    DATA : error is null 