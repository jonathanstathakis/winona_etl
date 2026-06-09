
  create view "winona_dw"."mart"."mart_wine_curr__dbt_tmp"
    
    
  as (
    /*
current wines.
*/
select
    *
from
    "winona_dw"."mart"."mart_item_curr"
where
    product_category = 'Wine'
  );