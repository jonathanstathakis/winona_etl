with seed_price_tier as (
    select
        *
    from
        "winona_dw"."public"."seed_price_tier"
)
select
    row_number() over () as id,*
from
    seed_price_tier -- TODO: continue defining valid values for the fields.