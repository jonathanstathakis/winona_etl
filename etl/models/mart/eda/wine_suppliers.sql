with wine as (
    select
        *
    from
        {{ ref("mart_wine_curr") }}
)
select
    supplier_name,
    count(*),
    avg(active)
from
    wine
where
    active = 1
group by
    supplier_name
order by count(*) desc
