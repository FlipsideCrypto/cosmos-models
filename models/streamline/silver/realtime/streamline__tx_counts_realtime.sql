{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"txcount_v2",
        "sql_limit" :"1000000",
        "producer_batch_size" :"2000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}" }
    )
) }}
-- depends_on: {{ ref('streamline__complete_tx_counts') }}
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        (
            block_number > 20637874
            OR block_number IN (
                15213800,
                17488639,
                17488648,
                17488944,
                17488961,
                17488964,
                17489210,
                17734128,
                17738941,
                18092650,
                18092959,
                18092999,
                18093205,
                18093461,
                18093862,
                18094000,
                18094147,
                18094348,
                18094403,
                18094564,
                18094669,
                18094751,
                18094886,
                18094904,
                18094948,
                18095037,
                18095718,
                18095974,
                18096277,
                18149517,
                18182229,
                18240778,
                18252837,
                18277408,
                18278205,
                18278469,
                18282210,
                18319662,
                18319711,
                18320890,
                18389757,
                18537771,
                18606443,
                18696110,
                18736547,
                18737141,
                18752879,
                18753526,
                18753910,
                18753915,
                18759707,
                18761074,
                18762540,
                18765703,
                18765816,
                18765858,
                18766423,
                18766574,
                18767446,
                18767839,
                18768922,
                18769886,
                18771023,
                18775951,
                18781033,
                18781406,
                18782616,
                18782639,
                18782679,
                18782702,
                18782720,
                18782733,
                18782754,
                18782782,
                18782805,
                18782815,
                18782825,
                18782839,
                18782859,
                18782880,
                18782894,
                18782940,
                18783128,
                18783142,
                18783404,
                18785658,
                18789994,
                18790396,
                18791369,
                18791380,
                18792124,
                18794759,
                18794864,
                18795583,
                18795990,
                18796279,
                18796507,
                18797103,
                18798511,
                18799797,
                18801793,
                18802153,
                18804315,
                18806632,
                18808772,
                18815042,
                18815760,
                18816273,
                18818258,
                18819193,
                18820744,
                18821573,
                18824715,
                18825069,
                18825779,
                18826090,
                18826763,
                18829883,
                18836468,
                18836477,
                18836728,
                18840477,
                18840961,
                18841307,
                18841967,
                18842733,
                18843028,
                18843041,
                18845382,
                18856153,
                18860793,
                18876652,
                18876737,
                18914352,
                18922450,
                18931191,
                18975671,
                18976135,
                18979869,
                18985111,
                18985805,
                18985807,
                18990736,
                18991599,
                19016808,
                19065130,
                19065331,
                19101632,
                19153683,
                19155009,
                19157384,
                19157713,
                19166208,
                19172620,
                19193228,
                19193229,
                19254797,
                19254827,
                19297427,
                19298638,
                19305839,
                19306131,
                19339346,
                19344764,
                19352383,
                19375506,
                19392815,
                19392823,
                19394069,
                19394320,
                19394324,
                19455070,
                19460500,
                19469138,
                19488488,
                19493674,
                19493680,
                19504381,
                19504842,
                19505013,
                19525159,
                19525162,
                19542651,
                19558138,
                19558532,
                19586994,
                19586998,
                19588508,
                19597121,
                19617322,
                19639602,
                19639609,
                19639610,
                19658277,
                19658279,
                19658284,
                19658291,
                19658294,
                19658306,
                19658311,
                19658332,
                19658345,
                19658459,
                19682654,
                19695914,
                19706281,
                19706282,
                19729448,
                19734478,
                19785792,
                19814922,
                19868973,
                19868974,
                19912105,
                19940534,
                19943248,
                19980626,
                19980639,
                20029424,
                20053365,
                20078352,
                20079693,
                20093929,
                20093931,
                20098412,
                20100856,
                20100865,
                20143152,
                20195615,
                20201245,
                20219641,
                20244255,
                20279253,
                20279300,
                20279304,
                20306935,
                20326668,
                20326671,
                20342184,
                20342410,
                20363353,
                20452472,
                20478751,
                20510095,
                20571005,
                20598876,
                20612217
            )
        )
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__complete_tx_counts") }}
),
{# retry AS (
SELECT
    NULL AS A.block_number
FROM
    {{ ref("streamline__complete_tx_counts") }} A
    JOIN {{ ref("silver__blockchain") }}
    b
    ON A.block_number = b.block_id
WHERE
    A.tx_count <> b.num_txs
),
#}
combo AS (
    SELECT
        block_number
    FROM
        blocks {# UNION
    SELECT
        block_number
    FROM
        retry #}
)
SELECT
    ROUND(
        block_number,
        -3
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{x-allthatnode-api-key}',
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
                '1',
                '1',
                'asc'
            )
        ),
        'vault/prod/cosmos/allthatnode/mainnet-archive/rpc'
    ) AS request,
    block_number
FROM
    combo
ORDER BY
    block_number
