
  create view "winona_dw"."mart"."x__dbt_tmp"
    
    
  as (
    with all_item as (
    select
        *
    from
        "winona_dw"."mart"."mart_item_curr_active"
)
select
name,
supplier_name,
inv_total,
supply_price,
retail_price,
retail_price/supply_price as markup_prop
-- mode() within group (order by retail_price/supply_price),
-- percentile_cont(0.5) within group (order by retail_price/supply_price)
-- -- mean(retail_price/supply_price)
from
    all_item
where
name ilike 'willy'
order by markup_prop desc

/*
calculations are whacky because spirits include RTDs which have a different markup rule.
*/
  );