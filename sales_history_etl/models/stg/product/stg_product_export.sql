select
    {{ dbt_utils.star(
        from = ref("raw_product_export"),
        except = ['tags']
    ) }},
    lower(tags) as tags
from
    {{ ref('raw_product_export') }}
