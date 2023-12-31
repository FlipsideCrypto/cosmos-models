{{ config(
    materialized = 'table',
    cluster_by = 'block_timestamp::DATE'
) }}

WITH base_transactions AS (

    SELECT
        block_number AS block_id,
        t.value :hash :: STRING AS tx_id,
        t.value :tx_result :codespace AS codespace,
        COALESCE(
            t.value :tx_result :gas_used,
            t.value :tx_result :gasUsed
        ) :: NUMBER AS gas_used,
        COALESCE(
            t.value :tx_result :gas_wanted,
            t.value :tx_result :gasWanted
        ) :: NUMBER AS gas_wanted,
        CASE
            WHEN t.value :tx_result :code :: NUMBER = 0 THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        t.value :tx_result :code :: NUMBER AS tx_code,
        COALESCE(
            t.value :tx_result :events,
            t.value :tx_result :tags
        ) AS msgs,
        t.value :tx_result :log :: STRING AS tx_log,
        t.value AS full_tx,
        _inserted_timestamp :: timestamp_ntz AS _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_FR_transactions_ch3') }},
        TABLE(FLATTEN(DATA :result :txs)) t
)
SELECT
    t.block_id,
    b.block_timestamp,
    tx_id,
    codespace,
    gas_used,
    gas_wanted,
    tx_succeeded,
    tx_code,
    msgs,
    tx_log,
    full_tx,
    concat_ws(
        '-',
        t.block_id,
        tx_id
    ) AS unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    t._inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_transactions t
    JOIN {{ ref('silver__blocks_ch3') }}
    b
    ON t.block_id = b.block_id qualify ROW_NUMBER() over (
        PARTITION BY tx_id
        ORDER BY
            t._inserted_timestamp DESC
    ) = 1
