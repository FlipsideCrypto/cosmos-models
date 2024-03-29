{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_get_cosmos_validators(object_construct('sql_source', '{{this.identifier}}'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(13) %}
    (

        SELECT
            {{ dbt_utils.generate_surrogate_key(
                ['block_number']
            ) }} AS id,
            block_number
        FROM
            {{ ref("streamline__blocks") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        EXCEPT
        SELECT
            id,
            block_number
        FROM
            {{ ref("streamline__complete_validators") }}
        WHERE
            block_number BETWEEN {{ item * 1000000 + 1 }}
            AND {{(
                item + 1
            ) * 1000000 }}
        ORDER BY
            block_number
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
