with seed_format as (
    select
        *
    from
        {{ ref('seed_format') }}
)
select
    row_number() over () as id,*
from
    seed_format -- TODO: continue defining valid values for the fields.
