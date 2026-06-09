WITH stg_product_export AS (
    SELECT
        *
    FROM
        "winona_dw"."stg"."stg_product_export"
),
composite AS (
    SELECT
        *
    FROM
        "winona_dw"."stg"."stg_composite"
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