
  create view "winona_dw"."stg"."stg_country__dbt_tmp"
    
    
  as (
    with seed_country as (
    select
        *
    from
        "winona_dw"."public"."seed_country"
)
select
    row_number() over () as id,*
from
    seed_country -- TODO: continue defining valid values for the fields.
  );