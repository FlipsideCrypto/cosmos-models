-- depends_on: {{ ref('bronze__streamline_transactions') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "complete_transactions_id",
    cluster_by = "ROUND(block_number, -3)",
    merge_exclude_columns = ["inserted_timestamp"],
) }}

SELECT
    COALESCE(
        VALUE :BLOCK_NUMBER_REQUESTED,
        DATA :height,
        VALUE :data :result :txs [0] :height
    ) :: INT AS block_number,
    COALESCE(
        VALUE :PAGE_NUMBER,
        metadata :request :params [2]
    ) :: INT AS page_number,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number','page_number']
    ) }} AS complete_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    file_name,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}
WHERE
    DATA <> '[]'

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(modified_timestamp) modified_timestamp
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY complete_transactions_id
ORDER BY
    inserted_timestamp DESC)) = 1
