{{ config(
    materialized = 'incremental',
    unique_key = 'block_id',
    cluster_by = ['_inserted_timestamp::date']
) }}
-- depends_on: {{ ref('bronze__streamline_blocks') }}
-- depends_on: {{ ref('bronze__streamline_FR_blocks') }}

SELECT
    VALUE,
    partition_key AS _partition_by_block_id,
    DATA :block :header :height :: INT AS block_id,
    metadata,
    DATA,
    TO_TIMESTAMP(
        inserted_timestamp
    ) AS _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_blocks') }}
{% else %}
    {{ ref('bronze__streamline_FR_blocks') }}
{% endif %}

{% if is_incremental() %}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_id
ORDER BY
    _inserted_timestamp DESC)) = 1
