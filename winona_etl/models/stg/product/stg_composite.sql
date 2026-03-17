WITH stg_product_export AS (
    SELECT
        *
    FROM
        {{ ref('stg_product_export') }}
),
composite_sku AS (
    SELECT
        DISTINCT sku
    FROM
        stg_product_export
    WHERE
        composite_sku IS NOT NULL
),
composite AS (
    SELECT
        stg_product_export.*
    FROM
        composite_sku
        LEFT JOIN stg_product_export
        ON stg_product_export.sku = composite_sku.sku
    WHERE
        handle IS NOT NULL -- the composite item line is differed by the presence of a handle value.
)
SELECT
    *
FROM
    composite
