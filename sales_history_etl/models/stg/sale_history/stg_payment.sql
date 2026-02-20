WITH raw_payment AS (
    SELECT
        sale_id,
        DATE,
        receipt_number,
        paid,
        details
    FROM
        {{ ref("raw_sale_history_dump_dedup") }}
    WHERE
        line_type = 'Payment'
)
SELECT
    *
FROM
    raw_payment
