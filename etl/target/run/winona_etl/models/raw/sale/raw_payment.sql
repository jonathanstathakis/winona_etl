
  create view "winona_dw"."raw"."raw_payment__dbt_tmp"
    
    
  as (
    select
    *,
    insert_index as abs_row_idx
from "winona_dw"."raw"."sale_history_dump"
where line_type = 'Payment'
  );