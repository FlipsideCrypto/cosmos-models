{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_transactions(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(1300) %}
    (

        SELECT
            {{ dbt_utils.surrogate_key(
                ['block_number']
            ) }} AS id,
            block_number
        FROM
            {{ ref("streamline__blocks") }}
        WHERE
            block_number BETWEEN {{ item * 10000 + 1 }}
            AND {{(
                item + 1
            ) * 10000 }}
        EXCEPT
        SELECT
            id,
            block_number
        FROM
            {{ ref("streamline__complete_transactions") }}
        WHERE
            block_number BETWEEN {{ item * 10000 + 1 }}
            AND {{(
                item + 1
            ) * 10000 }}
        ORDER BY
            block_number
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
