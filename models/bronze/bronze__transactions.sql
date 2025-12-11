{{ config(
    materialized = 'incremental',
    incremental_predicates = ["dynamic_range_predicate", "partition_key"],
    unique_key = ['block_id_requested','tx_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['_inserted_timestamp::DATE' ],
    full_refresh = false
) }}
-- depends_on: {{ ref('bronze__streamline_transactions') }}
-- depends_on: {{ ref('bronze__streamline_FR_transactions') }}

SELECT
    COALESCE(
        DATA :height,
        t.value :height
    ) :: INT AS block_id,
    COALESCE(
        DATA :hash,
        t.value :hash
    ) :: STRING AS tx_id,
    COALESCE(
        DATA :index,
        t.index
    ) AS tx_index,
    COALESCE(
        DATA :tx_result :codespace,
        t.value :tx_result :codespace
    ) :: STRING AS codespace,
    COALESCE(
        DATA :tx_result :gas_used,
        t.value :tx_result :gas_used
    ) :: NUMBER AS gas_used,
    COALESCE(
        DATA :tx_result :gas_wanted,
        t.value :tx_result :gas_wanted
    ) :: NUMBER AS gas_wanted,
    COALESCE(
        DATA :tx_result :code,
        t.value :tx_result :code
    ) :: INT AS tx_code,
    CASE
        WHEN NULLIF(
            tx_code,
            0
        ) IS NOT NULL THEN FALSE
        ELSE TRUE
    END AS tx_succeeded,
    COALESCE(
        DATA :tx_result :events,
        t.value :tx_result :events
    ) AS msgs,
    COALESCE(
        TRY_PARSE_JSON(
            COALESCE(
                DATA :tx_result :log,
                t.value :tx_result :log
            )
        ),
        COALESCE(
            DATA :tx_result :log,
            t.value :tx_result :log
        )
    ) AS tx_log,
    CASE
        WHEN t.value IS NOT NULL THEN t.value
        ELSE DATA
    END AS DATA,
    partition_key,
    COALESCE(
        A.value :BLOCK_NUMBER_REQUESTED,
        REPLACE(
            metadata :request :params [0],
            'tx.height='
        )
    ) AS block_id_requested,
    inserted_timestamp AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id_requested','tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_transactions') }}
{% else %}
    {{ ref('bronze__streamline_FR_transactions') }}
{% endif %}

A
JOIN LATERAL FLATTEN(
    DATA :result :txs,
    outer => TRUE
) t
WHERE
    block_id > 20637874

{% if is_incremental() %}
AND inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id_requested, tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
