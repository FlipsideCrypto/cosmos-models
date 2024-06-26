{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks_v2",
        "sql_limit" :"5000",
        "producer_batch_size" :"20",
        "worker_batch_size" :"10",
        "sql_source" :"{{this.identifier}}" }
    )
) }}

WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_blocks") }}
)
SELECT
    ROUND(
        block_number,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_number,
            'jsonrpc',
            '2.0',
            'method',
            'block',
            'params',
            ARRAY_CONSTRUCT(
                block_number :: STRING
            )
        ),
        'vault/prod/cosmos/quicknode/mainnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_number
