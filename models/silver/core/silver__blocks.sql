{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    full_refresh = false
) }}

SELECT
    block_id,
    COALESCE(
        DATA :result :block :header :time :: TIMESTAMP,
        DATA :block :header :time :: TIMESTAMP,
        DATA :result :block :header :timestamp :: TIMESTAMP,
        DATA :block :header :timestamp :: TIMESTAMP
    ) AS block_timestamp,
    'cosmos' AS blockchain,
    COALESCE(
        DATA :result :block :header :chain_id :: STRING,
        DATA :block :header :chain_id :: STRING
    ) AS chain_id,
    COALESCE(
        ARRAY_SIZE(
            DATA :result :block :data :txs
        ) :: NUMBER,
        ARRAY_SIZE(
            DATA :block :data :txs
        ) :: NUMBER
    ) AS tx_count,
    COALESCE(
        DATA :result :block :header :proposer_address :: STRING,
        DATA :block :header :proposer_address :: STRING
    ) AS proposer_address,
    COALESCE(
        DATA :result :block :header :validators_hash :: STRING,
        DATA :block :header :validators_hash :: STRING
    ) AS validator_hash,
    COALESCE(
        DATA :result :block :header,
        DATA :block :header
    ) AS header,
    concat_ws(
        '-',
        chain_id,
        block_id
    ) AS _unique_key,
    {{ dbt_utils.generate_surrogate_key(
        ['_unique_key']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    _inserted_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('bronze__blocks') }}
WHERE
    VALUE :data :error IS NULL
    AND DATA :error IS NULL
    AND DATA :result :begin_block_events IS NULL

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY chain_id,
    block_id
    ORDER BY
        _inserted_timestamp DESC
) = 1
