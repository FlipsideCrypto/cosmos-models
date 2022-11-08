
  create or replace  view COSMOS_DEV.streamline.blocks
  
  copy grants as (
    







SELECT
    height as block_number
FROM
    TABLE(streamline.udtf_get_base_table(12000000))
WHERE
    height >= 1000000 -- Highest block the archive has available
  );
