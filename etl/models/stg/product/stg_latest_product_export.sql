with stg_item as (
    select
        *
    from
        {{ ref('stg_item') }}
)
select
    max(export_timestamp) as export_timestamp
from
    stg_item
