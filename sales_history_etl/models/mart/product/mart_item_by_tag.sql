/*
denormalised items over tags
*/
with tag as (
    select
        *
    from
        {{ ref('stg_tag') }}
),
tag_to_item as (
    select
        *
    from
        {{ ref('stg_tag_to_item') }}
),
item as (
    select
        *
    from
        {{ ref('stg_item_curr') }}
),
tag_joined as (
    select
        item.*,
        tag
    from
        tag_to_item
        left join item
        on tag_to_item.item_id = item.id
        left join tag
        on tag_to_item.tag_id = tag.id
)
select
    *
from
    tag_joined
order by
    name
limit
    10
