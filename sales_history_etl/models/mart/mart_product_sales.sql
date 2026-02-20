WITH sale AS (
    SELECT
        *
    FROM
        {{ ref('stg_sale') }}
),
sale_line AS (
    SELECT
        *
    FROM
        {{ ref('stg_sale_line') }}
),
item AS (
    SELECT
        *
    FROM
        {{ ref('stg_item') }}
),
joined AS (
    SELECT
        {{ dbt_utils.star(
            from = ref('stg_sale_line'),
            except = ['sale_id','date'],
            prefix = 'saleline_',
            relation_alias = 'sale_line'
        ) }},
        {{ dbt_utils.star(
            from = ref('stg_sale'),
            prefix = 'sale_',
            relation_alias = 'sale'
        ) }},
        {{ dbt_utils.star(
            from = ref('stg_item'),
            prefix = 'item_',
            relation_alias = 'item'
        ) }}
    FROM
        sale_line
        LEFT JOIN item
        ON item.sku = sale_line.sku
        LEFT JOIN sale
        ON sale_line.sale_id = sale.sale_id
)
SELECT
    *
FROM
    joined
