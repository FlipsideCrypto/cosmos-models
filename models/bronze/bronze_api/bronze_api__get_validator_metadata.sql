{{ config(
    materialized = 'table',
    post_hook = "call silver.sp_bulk_get_asset_metadata()",
    enabled = False
) }}

WITH call AS (

    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://api.cosmoscan.net/validators',{},{}
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
),
keep_last_if_failed AS (
    SELECT
        i.value :operator_address :: STRING AS address,
        i.value AS DATA,
        _inserted_timestamp,
        2 AS RANK
    FROM
        call,
        LATERAL FLATTEN(
            input => resp :data
        ) i
    WHERE
        address IS NOT NULL
    UNION ALL
    SELECT
        address,
        DATA,
        _inserted_timestamp,
        1 AS RANK
    FROM
        bronze_api.get_validator_metadata
)
SELECT
    address,
    DATA,
    _inserted_timestamp
FROM
    keep_last_if_failed A
    JOIN (
        SELECT
            MAX(RANK) max_rank
        FROM
            keep_last_if_failed
    ) b
    ON A.rank = b.max_rank
