with wine as (
    select
        *
    from
        {{ ref("mart_wine_curr") }}
)
select
    count(*)
from
    wine
where
    active = 1
