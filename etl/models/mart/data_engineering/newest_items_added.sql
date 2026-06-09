{{ config(
    enabled = false
) }}

with stg_item as (

    select
        *
    from
        {{ ref('stg_item') }}
),
latest_timestamp as (
    select
        distinct export_timestamp
    from
        stg_item
    order by
        export_timestamp
    limit
        1
), second_latest_timestamp as (
    select
        distinct export_timestamp
    from
        stg_item
    order by
        export_timestamp offset 1
    limit
        1
), latest_export as (
    select
        *
    from
        stg_item
        left join latest_timestamp
        on stg_item.export_timestamp = latest_timestamp.export_timestamp
    where
        latest_timestamp.export_timestamp is not null
),
second_latest_export as (
    select
        *
    from
        stg_item
        left join second_latest_timestamp
        on stg_item.export_timestamp = second_latest_timestamp.export_timestamp
    where
        second_latest_timestamp.export_timestamp is not null
)
select
    *
from
    latest_export
    /* TODO: find all products in latest_export not in second_latest_export
        */
