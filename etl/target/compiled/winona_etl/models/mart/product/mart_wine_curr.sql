/*
current wines.
*/
select
    *
from
    "winona_dw"."mart"."mart_item_curr"
where
    product_category = 'Wine'