
  create view "winona_dw"."mart"."mart_item__dbt_tmp"
    
    
  as (
    SELECT
    *
FROM
    "winona_dw"."stg"."stg_item"
  );