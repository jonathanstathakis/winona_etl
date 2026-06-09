
  create view "winona_dw"."mart"."real_wines_wines__dbt_tmp"
    
    
  as (
    select name,
sku, 
inv_total,
active from "winona_dw"."mart"."mart_wine_curr" where supplier_name = 'Real Wines' order by inv_total desc
  );