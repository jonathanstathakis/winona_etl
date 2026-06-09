/*
current wines.
*/
select
    *
from
    {{ ref("mart_item_curr") }}
where
    product_category = 'Wine'
