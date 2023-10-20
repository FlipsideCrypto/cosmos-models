{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    height as block_number
FROM
    TABLE(streamline.udtf_get_base_table(500042))
WHERE
    height between 0 and 500042 // https://hub.cosmos.network/main/roadmap/#cosmos-hub-summary