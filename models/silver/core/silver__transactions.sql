{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
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
        _inserted_timestamp
    FROM
        {{ ref('bronze__tx_search') }},
        TABLE(FLATTEN(DATA :result :txs)) t

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
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
    JOIN {{ ref('silver__blocks') }}
    b
    ON t.block_id = b.block_id

{% if is_incremental() %}
WHERE
    b._inserted_timestamp :: DATE >= (
        SELECT
            MAX(_inserted_timestamp) :: DATE - 2
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_id
    ORDER BY
        t._inserted_timestamp DESC
) = 1
