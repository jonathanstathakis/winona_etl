
  create view "winona_dw"."stg"."stg_latest_product_export__dbt_tmp"
    
    
  as (
    with stg_item as (
    select
        *
    from
        "winona_dw"."stg"."stg_item"
)
select
    max(export_timestamp) as export_timestamp
from
    stg_item
  );