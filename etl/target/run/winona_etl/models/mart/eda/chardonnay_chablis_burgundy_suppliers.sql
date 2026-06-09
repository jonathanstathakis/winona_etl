
  create view "winona_dw"."mart"."chardonnay_chablis_burgundy_suppliers__dbt_tmp"
    
    
  as (
    with item as (
    select
        *
    from
        "winona_dw"."mart"."mart_item_by_tag"
),
filtered as (
    select
        distinct
        on (id) *
    from
        item
    where
        tag = 'chardonnay'
        or tag = 'variety_Chardonnay'
        and active = 1
),
supplier_counts as (
    select
        supplier_name,
        count(*)
    from
        filtered
    group by
        supplier_name
    order by
        count(*) desc
)
select
    *
from
    supplier_counts
  );