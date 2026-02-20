WITH raw_sale AS (
    SELECT
        *
    FROM
        {{ ref("raw_sale") }}
)
SELECT
    {{ dbt_utils.star(
        from = ref('raw_sale'),
        except = ['paid','sku','accountcodesale','accountcodepurchase']
    ) }}
FROM
    raw_sale
