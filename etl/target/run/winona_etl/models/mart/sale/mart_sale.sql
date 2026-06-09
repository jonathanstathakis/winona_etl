
  create view "winona_dw"."mart"."mart_sale__dbt_tmp"
    
    
  as (
    SELECT
    *
FROM
    "winona_dw"."stg"."stg_sale"
  );