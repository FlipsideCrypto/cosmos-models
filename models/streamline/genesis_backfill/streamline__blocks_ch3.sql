{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    height as block_number
FROM
    TABLE(streamline.udtf_get_base_table(5200790))
WHERE
    height between 2902000 and 5200790 // https://hub.cosmos.network/main/roadmap/#cosmos-hub-summary