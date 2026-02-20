WITH product_export_raw AS (
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
        product_export_raw.*
    FROM
        product_export_raw
        LEFT JOIN composite
        ON product_export_raw.sku = composite.sku
    WHERE
        composite.sku IS NULL
)
SELECT
    *
FROM
    item
