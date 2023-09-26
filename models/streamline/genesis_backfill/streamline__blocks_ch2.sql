{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    height as block_number
FROM
    TABLE(streamline.udtf_get_base_table(2901999))
WHERE
    height between 500043 and 2901999 // https://hub.cosmos.network/main/roadmap/#cosmos-hub-summary