select * from {{ source('raw', 'product_export_dump') }}
