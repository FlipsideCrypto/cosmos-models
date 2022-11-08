

SELECT
    md5(cast(coalesce(cast(block_number as TEXT), '') as TEXT)) AS id,
    block_number
FROM
    COSMOS_DEV.streamline.blocks
WHERE
    block_number > 12000000
    AND block_number IS NOT NULL
EXCEPT
SELECT
    id,
    block_number
FROM
    COSMOS_DEV.streamline.complete_blocks
WHERE
    block_number > 12000000
