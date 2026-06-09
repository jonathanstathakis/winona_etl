
  create view "winona_dw"."mart"."wine_suppliers__dbt_tmp"
    
    
  as (
    with wine as (
    select
        *
    from
        "winona_dw"."mart"."mart_wine_curr"
)
select
    supplier_name,
    count(*),
    avg(active)
from
    wine
where
    active = 1
group by
    supplier_name
order by count(*) desc
  );