{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"transactions_v2",
        "sql_limit" :"10",
        "producer_batch_size" :"5",
        "worker_batch_size" :"1",
        "exploded_key": "[\"result.txs\"]",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__complete_transactions') }}
-- depends_on: {{ ref('streamline__complete_tx_counts') }}
WITH blocks AS (

    SELECT
        A.block_number,
        tx_count
    FROM
        {{ ref("streamline__complete_tx_counts") }} A
    WHERE
        block_number IN (
            20745901,
            20746801,
            20748924,
            20759934,
            20778140,
            20784319,
            20818327,
            20826784,
            20843581,
            20857395,
            20861200,
            20864260
        )
),
numbers AS (
    SELECT
        _id AS page_number
    FROM
        {{ source(
            'crosschain_silver',
            'number_sequence'
        ) }}
    WHERE
        _id < 2000
),
blocks_with_page_numbers AS (
    SELECT
        b.block_number :: INT AS block_number,
        n.page_number
    FROM
        numbers n
        JOIN blocks b
        ON n.page_number <= b.tx_count
    EXCEPT
    SELECT
        block_number,
        page_number
    FROM
        {{ ref("streamline__complete_transactions") }}
)
SELECT
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    live.udf_api(
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
            'tx_search',
            'params',
            ARRAY_CONSTRUCT(
                'tx.height=' || block_number :: STRING,
                TRUE,
                page_number :: STRING,
                '1',
                'asc'
            )
        ),
        'vault/prod/cosmos/quicknode/mainnet'
    ) AS request,
    page_number,
    block_number AS block_number_requested
FROM
    blocks_with_page_numbers
ORDER BY
    block_number,
    page_number
