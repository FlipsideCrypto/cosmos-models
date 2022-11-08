

WITH meta AS (

    SELECT
        last_modified,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => 'streamline.COSMOS_DEV.blocks'
            )
        ) A
)


    SELECT
        md5(cast(coalesce(cast(block_number as TEXT), '') as TEXT)) AS id,
        block_number,
        last_modified AS _inserted_timestamp
    FROM
        streamline.COSMOS_DEV.blocks
        JOIN meta b
        ON b.file_name = metadata$filename



qualify(ROW_NUMBER() over (PARTITION BY id
ORDER BY
    _inserted_timestamp DESC)) = 1