
  create view "winona_dw"."stg"."stg_display_cat__dbt_tmp"
    
    
  as (
    with seed_display_cat as (
    select
        *
    from
        "winona_dw"."public"."seed_display_cat"
)
select
    row_number() over () as id,*
from
    seed_display_cat -- TODO: continue defining valid values for the fields.
  );