{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
    block_id,
    COALESCE(
        data :result :block :header :time :: TIMESTAMP,
        data :block :header :time :: TIMESTAMP,
        data :result :block :header :timestamp :: TIMESTAMP,
        data :block :header :timestamp :: TIMESTAMP
    ) AS block_timestamp,
    COALESCE(
        data :result :block :header :chain_id :: STRING,
        data :block :header :chain_id :: STRING
    ) AS chain_id,
    COALESCE(
        ARRAY_SIZE(
            data :result :block :data :txs
        ) :: NUMBER,
        ARRAY_SIZE(
            data :block :data :txs
        ) :: NUMBER
    ) AS tx_count,
    COALESCE(
        data :result :block :header :proposer_address :: STRING,
        data :block :header :proposer_address :: STRING
    ) AS proposer_address,
    COALESCE(
        data :result :block :header :validators_hash :: STRING,
        data :block :header :validators_hash :: STRING
    ) AS validator_hash,
    COALESCE(
        data :result :block :header,
        data :block :header
    ) AS header,
    _partition_by_block_id,
    concat_ws(
        '-',
        chain_id,
        block_id
    ) AS _unique_key
FROM
    {{ ref('bronze__blocks') }}
WHERE
    value :data :error IS NULL
    AND data :error IS NULL
    AND data :result :begin_block_events is null 

{% if is_incremental() %}
AND _partition_by_block_id >= (
    SELECT
        MAX(_partition_by_block_id)
    FROM
        {{ this }}
)
{% endif %}

qualify row_number() over (partition by chain_id, block_id 
    order by _partition_by_block_id desc) = 1 
    