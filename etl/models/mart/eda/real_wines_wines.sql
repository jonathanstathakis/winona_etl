select name,
sku, 
inv_total,
active from {{ref('mart_wine_curr')}} where supplier_name = 'Real Wines' order by inv_total desc