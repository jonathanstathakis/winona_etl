select
    *
from
    {{ ref('mart_item_curr') }}
where
    active = 1
