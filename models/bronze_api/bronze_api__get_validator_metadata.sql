{{ config(
  materialized = 'table'
) }}

WITH call AS (

    SELECT
        ethereum.streamline.udf_api(
            'GET',
            'https://api.cosmoscan.net/validators',{},{}
        ) AS resp,
        SYSDATE() AS _inserted_timestamp
)
SELECT
    i.value :operator_address :: STRING as address, 
    i.value AS DATA,
    _inserted_timestamp
FROM
    call,
    LATERAL FLATTEN(
        input => resp :data
    ) i
