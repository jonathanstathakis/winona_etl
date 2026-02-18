WITH raw_sale_line AS (
    SELECT
        DATE,
        note,
        quantity,
        subtotal,
        sales_tax,
        discount,
        loyalty,
        total,
        details,
        sku,
        accountcodesale
    FROM
        {{ ref("raw_sale_history_dump_dedup") }}
    WHERE
        line_type = 'Sale Line'
)
SELECT
    *
FROM
    raw_sale_line
