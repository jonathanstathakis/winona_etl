with stg_product_export as (
    select
        *
    from
        {{ ref('stg_product_export') }}
),
/*
interesting problem here is that because the exports are by their nature out of sync with the current state,
more so the further back you go, tags may exist in older exports that do not exist in the current iteration.
The solution to this would be a sub-routine that checks stored tags against the latest export and marks tags as
inactive if they are not in the current iteration. or something like that.

*/
split_tag as (
    select
        export_timestamp,
        id as item_id,
        regexp_split_to_table(
            tags,
            ';'
        ) as tag
    from
        stg_product_export
),
tag_to_item_val as (
    select
        distinct tag,
        item_id
    from
        split_tag
),
stg_tag as (
    select
        *
    from
        {{ ref('stg_tag') }}
),
item_to_tag_id as (
    select
        stg_tag.id as tag_id,
        tag_to_item_val.item_id
    from
        tag_to_item_val
        left join stg_tag
        on tag_to_item_val.tag = stg_tag.tag
)
select
    *
from
    item_to_tag_id
