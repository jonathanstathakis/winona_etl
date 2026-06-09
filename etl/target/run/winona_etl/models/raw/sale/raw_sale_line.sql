
  create view "winona_dw"."raw"."raw_sale_line__dbt_tmp"
    
    
  as (
    select * from "winona_dw"."raw"."sale_history_dump"
where line_type = 'Sale Line'
  );