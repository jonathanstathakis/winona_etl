WITH stg_product_export AS (
    SELECT
        *
    FROM
        {{ ref('stg_product_export') }}
),
composite AS (
    SELECT
        *
    FROM
        {{ ref('stg_composite') }}
),
item AS (
    SELECT
        stg_product_export.*
    FROM
        stg_product_export
        LEFT JOIN composite
        ON stg_product_export.sku = composite.sku
    WHERE
        composite.sku IS NULL
)
select
    *
from
    item
