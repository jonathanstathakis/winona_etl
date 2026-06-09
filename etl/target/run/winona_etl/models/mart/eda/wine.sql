
  create view "winona_dw"."mart"."wine__dbt_tmp"
    
    
  as (
    with wine as (
    select
        *
    from
        "winona_dw"."mart"."mart_wine_curr"
)
select
    count(*)
from
    wine
where
    active = 1
  );