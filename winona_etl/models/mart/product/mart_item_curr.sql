SELECT
    *,
    to_char(to_timestamp(export_timestamp), 'DD Mon YYYY HH24:MI') AS export_date
FROM
    {{ ref('stg_item_curr') }}
