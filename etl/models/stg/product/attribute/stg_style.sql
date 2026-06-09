with seed_style as (
    select
        *
    from
        {{ ref('seed_style') }}
)
select
    row_number() over () as id,*
from
    seed_style -- TODO: continue defining valid values for the fields.
