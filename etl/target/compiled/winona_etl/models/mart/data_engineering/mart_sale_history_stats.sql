/*
Provide statistics on the sale history export. earliest and latest date, number of rows
is a good start
*/
WITH stg_sale AS (
    SELECT
        *
    FROM
        "winona_dw"."stg"."stg_sale"
)
SELECT
    MIN(DATE) AS earliest_date,
    MAX(DATE) AS latest_date,
    COUNT(*) AS num_sales
FROM
    stg_sale