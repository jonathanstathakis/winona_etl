with unique_item as (
    select
        {{ dbt_utils.star(
            from = ref('raw_unique_item'),
            except = ['composite_name','composite_sku','composite_quantity']
        ) }}
    from
        {{ ref('raw_unique_item') }}
)
select
    *
from
    unique_item
