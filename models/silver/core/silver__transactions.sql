{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

WITH old_base_transactions AS (

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
            DATEADD('minute', -15, MAX(_inserted_timestamp))
        FROM
            {{ this }})
        {% endif %}
    ),
    base_transactions AS (
        SELECT
            block_id,
            tx_id,
            codespace :: variant AS codespace,
            gas_used,
            gas_wanted,
            tx_succeeded,
            tx_code,
            msgs,
            tx_log :: STRING AS tx_log,
            TO_TIMESTAMP(
                _inserted_timestamp
            ) AS _inserted_timestamp
        FROM
            {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            DATEADD('minute', -90, MAX(_inserted_timestamp))
        FROM
            {{ this }})
        {% endif %}
    ),
    combo AS (
        SELECT
            block_id,
            tx_id,
            codespace,
            gas_used,
            gas_wanted,
            tx_succeeded,
            tx_code,
            msgs,
            tx_log,
            _inserted_timestamp
        FROM
            base_transactions
        UNION ALL
        SELECT
            block_id,
            tx_id,
            codespace,
            gas_used,
            gas_wanted,
            tx_succeeded,
            tx_code,
            msgs,
            tx_log,
            _inserted_timestamp
        FROM
            old_base_transactions
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
    tx_log :: STRING AS tx_log,
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
    ON t.block_id = b.block_id qualify ROW_NUMBER() over (
        PARTITION BY tx_id
        ORDER BY
            t._inserted_timestamp DESC
    ) = 1
