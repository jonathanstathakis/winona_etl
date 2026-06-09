
  create view "winona_dw"."stg"."stg_colour__dbt_tmp"
    
    
  as (
    with seed_colour as (
    select
        *
    from
        "winona_dw"."public"."seed_colour"
)
select
    row_number() over () as id,*
from
    seed_colour -- TODO: continue defining valid values for the fields.
  );