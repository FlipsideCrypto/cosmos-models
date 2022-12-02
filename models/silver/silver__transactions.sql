{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = 'block_timestamp::DATE',
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

with base_transactions as (
    SELECT 
    block_id, 
    t.value :hash :: STRING as tx_id, 
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
        {{ ref('bronze__tx_search') }}
        , TABLE(FLATTEN(data :result :txs)) t
    
    )


    select 
        t.block_id, 
        b.block_timestamp, 
        tx_id, 
        'cosmos' as blockchain,
        b.chain_id,
        codespace,
        gas_used,
        gas_wanted,
        tx_succeeded,
        tx_code,
        msgs,
        tx_log,
        t._partition_by_block_id
        from base_transactions t 
        join {{ ref('silver__blocks') }} b 
             on t.block_id = b.block_id 
        