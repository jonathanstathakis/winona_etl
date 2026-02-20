WITH raw_product_export AS (
    SELECT
        *
    FROM
        {{ ref('raw_product_export') }}
),
composite AS (
    SELECT
        *
    FROM
        {{ ref('stg_composite') }}
),
item AS (
    SELECT
        *
    FROM
        {{ ref('stg_item') }}
),
composite_items AS (
    SELECT
        raw_product_export.*
    FROM
        raw_product_export
        LEFT JOIN composite
        ON raw_product_export.sku = composite.sku
    WHERE
        --     composite.sku IS NOT NULL
        --     AND
        raw_product_export.handle IS NULL
),
composite_id_to_item_id AS (
    SELECT
        A.id AS composite_id,
        b.id AS item_id,*
    FROM
        composite_items
        LEFT JOIN stg_composite A
        ON composite_items.sku = A.sku
        LEFT JOIN stg_item b
        ON composite_items.composite_sku = b.sku
),
distinct_tuple AS (
    SELECT
        DISTINCT composite_id,
        item_id
    FROM
        composite_id_to_item_id
)
SELECT
    *
FROM
    distinct_tuple
