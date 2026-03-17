with seed_price_tier as (
    select
        *
    from
        {{ ref('seed_price_tier') }}
)
select
    row_number() over () as id,*
from
    seed_price_tier -- TODO: continue defining valid values for the fields.
