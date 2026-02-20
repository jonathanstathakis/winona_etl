with raw_sale_line as (
    SELECT
        *
    from
        {{ ref('raw_sale_line') }}
)
SELECT
    {{ dbt_utils.star(
        from = ref('raw_sale_line'),
        except = ['customer_code','customer_name','note','paid','register','status','accountcodepurchase','state','attributes']
    ) }}
FROM
    raw_sale_line
