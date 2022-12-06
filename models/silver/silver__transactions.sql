{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

WITH base_transactions AS (

    SELECT
        block_id,
        t.value :hash :: STRING AS tx_id,
        t.value :tx_result :codespace AS codespace,
        t.value :tx_result :gas_used :: NUMBER AS gas_used,
        t.value :tx_result :gas_wanted :: NUMBER AS gas_wanted,
        CASE
            WHEN t.value :tx_result :code :: NUMBER = 0 THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        t.value :tx_result :code :: NUMBER AS tx_code,
        t.value :tx_result :events AS msgs,
        t.value :tx_result :log :: STRING AS tx_log,
        _partition_by_block_id
    FROM
        {{ ref('bronze__tx_search') }},
        TABLE(FLATTEN(DATA :result :txs)) t

{% if is_incremental() %}
WHERE
    _partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id)
        FROM
            {{ this }}
    )
{% endif %}
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
    t._partition_by_block_id,
    concat_ws(
        '-',
        t.block_id,
        tx_id
    ) AS _unique_key
FROM
    base_transactions t
    JOIN {{ ref('silver__blocks') }}
    b
    ON t.block_id = b.block_id

{% if is_incremental() %}
WHERE
    b._partition_by_block_id >= (
        SELECT
            MAX(_partition_by_block_id)
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY t.block_id,
    tx_id
    ORDER BY
        t._partition_by_block_id DESC
) = 1
