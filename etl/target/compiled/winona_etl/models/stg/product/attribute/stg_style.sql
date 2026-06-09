with seed_style as (
    select
        *
    from
        "winona_dw"."public"."seed_style"
)
select
    row_number() over () as id,*
from
    seed_style -- TODO: continue defining valid values for the fields.