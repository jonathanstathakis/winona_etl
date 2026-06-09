
  create view "winona_dw"."raw"."raw_product_export__dbt_tmp"
    
    
  as (
    select * from "winona_dw"."raw"."product_export_dump"
  );