{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_transactions(object_construct('sql_source', '{{this.identifier}}','batch_call_limit','100','producer_batch_size', {{var('producer_batch_size','1650000')}}, 'worker_batch_size', {{var('worker_batch_size','8250')}}))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS id,
    block_number
FROM
    {{ ref("streamline__blocks") }}
WHERE
    block_number between 17448785 and 17584836
    -- block_number between 17204185 and 17204388
    -- block_number between 17199205 and 17199922
    AND block_number IS NOT NULL