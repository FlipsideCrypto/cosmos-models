{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_transactions(object_construct('sql_source', '{{this.identifier}}','batch_call_limit','10','producer_batch_size', {{var('producer_batch_size','2000')}}, 'worker_batch_size', {{var('worker_batch_size','200')}}))",
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
    block_number > 6754140 {# block_number > 13000000 #}
    AND block_number IS NOT NULL
EXCEPT
SELECT
    id,
    block_number
FROM
    {{ ref("streamline__complete_transactions") }}
WHERE
    block_number > 6754140 {# block_number > 13000000 #}
