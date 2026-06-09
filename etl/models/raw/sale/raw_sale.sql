select * from {{ source('raw', 'sale_history_dump') }}
where line_type = 'Sale'
