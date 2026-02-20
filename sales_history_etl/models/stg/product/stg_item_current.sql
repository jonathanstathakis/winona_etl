with stg_item as (
    select
        *
    from
        {{ ref('stg_item') }}
),
latest_export_timestamp as (
    select
        *
    from
        {{ ref('stg_latest_product_export') }}
)
select
    stg_item.*
from
    stg_item
    left join latest_export_timestamp
    on stg_item.export_timestamp = latest_export_timestamp.export_timestamp
where
    latest_export_timestamp.export_timestamp is not null
