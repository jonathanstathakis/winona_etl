
  create view "winona_dw"."stg"."stg_region__dbt_tmp"
    
    
  as (
    with seed_region as (
    select
        *
    from
        "winona_dw"."public"."seed_region"
)
select
    row_number() over () as id,*
from
    seed_region -- TODO: continue defining valid values for the fields.
  );