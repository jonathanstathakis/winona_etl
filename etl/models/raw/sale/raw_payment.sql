select
    *,
    insert_index as abs_row_idx
from {{ source('raw', 'sale_history_dump') }}
where line_type = 'Payment'
