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