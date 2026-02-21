with unique_item as (
    select
        {{ dbt_utils.star(
            from = ref('stg_unique_item'),
            except = ['composite_name','composite_sku','composite_quantity']
        ) }}
    from
        {{ ref('stg_unique_item') }}
)
select
    *
from
    unique_item
