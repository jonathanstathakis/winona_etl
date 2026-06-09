SELECT
    *,
    to_char(to_timestamp(export_timestamp), 'DD Mon YYYY HH24:MI') AS export_date,
    coalesce(inventory_winona_avalon, 0)
    + coalesce(inventory_winona_manly, 0)
    + coalesce(inventory_winona_rozelle, 0)
    + coalesce(inventory_winona_warehouse, 0) AS inv_total
FROM
    "winona_dw"."stg"."stg_item_curr"