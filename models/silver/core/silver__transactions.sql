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
        tx_id,
        codespace :: variant AS codespace,
        gas_used,
        gas_wanted,
        tx_succeeded,
        tx_code,
        msgs,
        tx_log,
        TO_TIMESTAMP(
            _inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ ref('bronze__transactions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp :: DATE >= (
        SELECT
            DATEADD('minute', -15, MAX(_inserted_timestamp))
        FROM
            {{ this }})
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
