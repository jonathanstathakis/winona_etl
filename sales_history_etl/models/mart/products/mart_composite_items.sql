WITH composite AS (
    SELECT
        *
    FROM
        {{ ref('stg_composite') }}
),
composite_to_item AS (
    SELECT
        *
    FROM
        {{ ref('stg_composite_to_item') }}
),
item AS (
    SELECT
        *
    FROM
        {{ ref('stg_item') }}
),
joined AS (
    SELECT
        composite.name AS composite_name,
        composite.sku AS composite_sku,*
    FROM
        composite_to_item
        LEFT JOIN composite
        ON composite_to_item.composite_id = composite.id
        LEFT JOIN item
        ON composite_to_item.item_id = item.id
)
SELECT
    *
FROM
    joined
