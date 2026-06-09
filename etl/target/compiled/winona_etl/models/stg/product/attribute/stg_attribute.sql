/* product skus and their attributes (wine)
*/
with wine_curr as (
    select
        id,
        name,
        tags
    from
        "winona_dw"."stg"."stg_item_curr"
    where
        product_category = 'Wine'
        and active = 1
),
with_colour as (
    select
        *,
        case
            when tags ilike '%white%' then 'white'
            when tags ilike '%red wine%' then 'red'
            when tags ilike '%rose wine%' then 'rose'
            when tags ilike '%orange wine%' then 'orange'
            when name ilike '%rose%' then 'rose'
            when name ilike '%amaury beaufort%le jardinot%champagne%' then 'white'
            when name ilike '%The Other Right - %Bright Young Thing%' then 'rose'
            when name ilike '%rosa per voi%' then 'rose'
            when name ilike '%koppitsch ''touch%' then 'white'
            when name ilike '%purellus%' then 'white'
            when name ilike '%trossecco%' then 'white'
            when name ilike '%amato vino ''lava sunset''%' then 'white'
            when name ilike '%bianco%' then 'white'
            when name ilike '%blanc de blanc%' then 'white'
            when name ilike '%sparkling vouvray%' then 'white'
            when tags ilike '%shiraz%' then 'red'
            when tags ilike '%red%' then 'red'
        end as colour
    from
        wine_curr
)
select
    name,
    tags
from
    with_colour
where
    colour is null -- TODO: continue labelling, realise its all futile because your boss is a moron. Continue.