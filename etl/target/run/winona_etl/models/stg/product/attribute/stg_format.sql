
  create view "winona_dw"."stg"."stg_format__dbt_tmp"
    
    
  as (
    with seed_format as (
    select
        *
    from
        "winona_dw"."public"."seed_format"
)
select
    row_number() over () as id,*
from
    seed_format -- TODO: continue defining valid values for the fields.
  );