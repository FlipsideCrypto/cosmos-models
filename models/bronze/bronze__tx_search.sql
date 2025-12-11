{{ config(
    materialized = 'incremental',
    unique_key = ['block_id_requested','unique_key'],
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::date'],
    full_refresh = false
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
-- depends_on: {{ ref('bronze__streamline_FR_transactions') }}

SELECT
    VALUE,
    partition_key AS _partition_by_block_id,
    DATA :height :: INT AS block_id,
    REPLACE(
        metadata :request :params [0],
        'tx.height='
    ) :: INT AS block_id_requested,
    metadata,
    DATA,
    DATA :id AS unique_key,
    TO_TIMESTAMP(
        inserted_timestamp
    ) AS _inserted_timestamp
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}
WHERE
    block_id <= 20637874

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
