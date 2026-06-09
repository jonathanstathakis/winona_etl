
  create view "winona_dw"."mart"."mart_item_curr_active__dbt_tmp"
    
    
  as (
    select
    *
from
    "winona_dw"."mart"."mart_item_curr"
where
    active = 1
  );