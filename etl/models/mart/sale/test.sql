select
'hi',
*
from
{{ ref("stg_sale")}}
