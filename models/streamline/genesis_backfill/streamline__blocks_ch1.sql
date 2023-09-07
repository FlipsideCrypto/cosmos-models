{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    height as block_number
FROM
    TABLE(streamline.udtf_get_base_table(35))
WHERE
    height between 0 and 20 