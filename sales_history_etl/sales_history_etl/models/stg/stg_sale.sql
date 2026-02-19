WITH raw_sale AS (
    SELECT
        -- all other columns are null for line_type = 'Sale'
        DATE,
        receipt_number,
        line_type,
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
        {{ ref("raw_sale_history_dump") }}
    WHERE
        line_type = 'Sale'
)
SELECT
    *
FROM
    raw_sale
