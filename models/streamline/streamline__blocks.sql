{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}


{% if execute %}
{% set height = run_query('SELECT streamline.udf_get_cosmos_chainhead()') %}
{% set block_height = height.columns[0].values()[0] %}
{% else %}
{% set block_height = 13000000 %}
{% endif %}

SELECT
    height as block_number
FROM
    TABLE(streamline.udtf_get_base_table({{block_height}}))
WHERE
    height >= 5200791 -- Highest block the archive has available