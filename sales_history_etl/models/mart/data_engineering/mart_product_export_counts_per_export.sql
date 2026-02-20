with stg_item as (
    select
        *
    from
        {{ ref('stg_item') }}
)
select
    to_timestamp(export_timestamp) as export_timestamp,
    count(*) as items_per_export
from
    stg_item
group by
    export_timestamp
order by
    export_timestamp
