with seed_region as (
    select
        *
    from
        {{ ref('seed_region') }}
)
select
    row_number() over () as id,*
from
    seed_region -- TODO: continue defining valid values for the fields.
