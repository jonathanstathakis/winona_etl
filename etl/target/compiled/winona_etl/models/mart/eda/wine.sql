with wine as (
    select
        *
    from
        "winona_dw"."mart"."mart_wine_curr"
)
select
    count(*)
from
    wine
where
    active = 1