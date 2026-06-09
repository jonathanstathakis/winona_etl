
  create view "winona_dw"."stg"."stg_payment__dbt_tmp"
    
    
  as (
    with raw_payment as (
    select
        *
    from
        "winona_dw"."raw"."raw_payment"
)
select
    "sale_history_line_id",
  "insert_index",
  "export_timestamp",
  "export_filename",
  "outlet",
  "date",
  "receipt_number",
  "line_type",
  "paid",
  "details",
  "_user",
  "abs_row_idx"
from
    raw_payment
order by
    abs_row_idx
  );