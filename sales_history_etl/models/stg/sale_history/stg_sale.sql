WITH raw_sale AS (
    SELECT
        -- all other columns are null for line_type = 'Sale'
        sale_id,
        DATE,
        receipt_number,
        customer_code,
        customer_name,
        note,
        quantity,
        subtotal,
        sales_tax,
        discount,
        loyalty,
        total,
        details,
        register,
        _user,
        status,
        state,
        attributes
    FROM
        {{ ref("raw_sale_history_dump_dedup") }}
    WHERE
        line_type = 'Sale'
)
SELECT
    *
FROM
    raw_sale
