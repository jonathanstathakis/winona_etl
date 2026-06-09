WITH sale AS (
    SELECT
        *
    FROM
        "winona_dw"."stg"."stg_sale"
),
sale_line AS (
    SELECT
        *
    FROM
        "winona_dw"."stg"."stg_sale_line"
),
item AS (
    SELECT
        *
    FROM
        "winona_dw"."stg"."stg_item"
),
joined AS (
    SELECT
        sale_line."sale_history_line_id" as "saleline_sale_history_line_id",
  sale_line."insert_index" as "saleline_insert_index",
  sale_line."export_timestamp" as "saleline_export_timestamp",
  sale_line."export_filename" as "saleline_export_filename",
  sale_line."outlet" as "saleline_outlet",
  sale_line."line_type" as "saleline_line_type",
  sale_line."quantity" as "saleline_quantity",
  sale_line."subtotal" as "saleline_subtotal",
  sale_line."sales_tax" as "saleline_sales_tax",
  sale_line."discount" as "saleline_discount",
  sale_line."loyalty" as "saleline_loyalty",
  sale_line."total" as "saleline_total",
  sale_line."details" as "saleline_details",
  sale_line."_user" as "saleline__user",
  sale_line."sku" as "saleline_sku",
  sale_line."accountcodesale" as "saleline_accountcodesale",
        sale."sale_history_line_id" as "sale_sale_history_line_id",
  sale."insert_index" as "sale_insert_index",
  sale."export_timestamp" as "sale_export_timestamp",
  sale."export_filename" as "sale_export_filename",
  sale."outlet" as "sale_outlet",
  sale."date" as "sale_date",
  sale."receipt_number" as "sale_receipt_number",
  sale."line_type" as "sale_line_type",
  sale."customer_code" as "sale_customer_code",
  sale."customer_name" as "sale_customer_name",
  sale."note" as "sale_note",
  sale."quantity" as "sale_quantity",
  sale."subtotal" as "sale_subtotal",
  sale."sales_tax" as "sale_sales_tax",
  sale."discount" as "sale_discount",
  sale."loyalty" as "sale_loyalty",
  sale."total" as "sale_total",
  sale."details" as "sale_details",
  sale."register" as "sale_register",
  sale."_user" as "sale__user",
  sale."status" as "sale_status",
  sale."state" as "sale_state",
  sale."attributes" as "sale_attributes",
        item."pk" as "item_pk",
  item."id" as "item_id",
  item."handle" as "item_handle",
  item."sku" as "item_sku",
  item."name" as "item_name",
  item."description" as "item_description",
  item."product_category" as "item_product_category",
  item."variant_option_one_name" as "item_variant_option_one_name",
  item."variant_option_one_value" as "item_variant_option_one_value",
  item."variant_option_two_name" as "item_variant_option_two_name",
  item."variant_option_two_value" as "item_variant_option_two_value",
  item."variant_option_three_name" as "item_variant_option_three_name",
  item."variant_option_three_value" as "item_variant_option_three_value",
  item."supply_price" as "item_supply_price",
  item."retail_price" as "item_retail_price",
  item."loyalty_value" as "item_loyalty_value",
  item."loyalty_value_default" as "item_loyalty_value_default",
  item."tax_name" as "item_tax_name",
  item."tax_value" as "item_tax_value",
  item."account_code" as "item_account_code",
  item."account_code_purchase" as "item_account_code_purchase",
  item."brand_name" as "item_brand_name",
  item."supplier_name" as "item_supplier_name",
  item."supplier_code" as "item_supplier_code",
  item."active" as "item_active",
  item."track_inventory" as "item_track_inventory",
  item."inventory_winona_avalon" as "item_inventory_winona_avalon",
  item."reorder_point_winona_avalon" as "item_reorder_point_winona_avalon",
  item."restock_level_winona_avalon" as "item_restock_level_winona_avalon",
  item."inventory_winona_manly" as "item_inventory_winona_manly",
  item."reorder_point_winona_manly" as "item_reorder_point_winona_manly",
  item."restock_level_winona_manly" as "item_restock_level_winona_manly",
  item."inventory_winona_rozelle" as "item_inventory_winona_rozelle",
  item."reorder_point_winona_rozelle" as "item_reorder_point_winona_rozelle",
  item."restock_level_winona_rozelle" as "item_restock_level_winona_rozelle",
  item."inventory_winona_warehouse" as "item_inventory_winona_warehouse",
  item."reorder_point_winona_warehouse" as "item_reorder_point_winona_warehouse",
  item."restock_level_winona_warehouse" as "item_restock_level_winona_warehouse",
  item."export_timestamp" as "item_export_timestamp",
  item."tags" as "item_tags"
    FROM
        sale_line
        LEFT JOIN item
        ON item.sku = sale_line.sku
        LEFT JOIN sale
        ON sale_line.receipt_number = sale.receipt_number
)
SELECT
    *
FROM
    joined