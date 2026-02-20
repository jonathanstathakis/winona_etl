with stg_item as (
    select
        *
    from
        {{ ref('stg_item') }}
)
select
    count(*) as total_rowcount,
    min(to_timestamp(export_timestamp)) as earliest_export_dt,
    max(to_timestamp(export_timestamp)) as latest_record_dt
from
    stg_item
