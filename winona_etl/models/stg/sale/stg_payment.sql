with raw_payment as (
    select
        *
    from
        {{ ref("raw_payment") }}
)
select
    {{ dbt_utils.star(
        from = ref("raw_payment"),
        except = ['customer_code','customer_name','note','quantity','subtotal','sales_tax','discount','loyalty','total','register','status','sku','accountcodesale','accountcodepurchase','state','attributes']
    ) }}
from
    raw_payment
order by
    abs_row_idx
