
  create view "winona_dw"."stg"."stg_variety__dbt_tmp"
    
    
  as (
    with seed_variety as (
    select
        *
    from
        "winona_dw"."public"."seed_variety"
)
select
    row_number() over () as id,*
from
    seed_variety -- TODO: continue defining valid values for the fields.
  );