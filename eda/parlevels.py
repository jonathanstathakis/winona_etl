import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    from db import connect
    return connect, mo


@app.cell
def _(connect):
    conn = connect()
    return (conn,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        select
            name,
            reorder_point_Winona_Rozelle,
            restock_level_Winona_Rozelle,

        from
            wh.mart.mart_item_curr_active
        where
            product_category ilike '%beer%'
            and
            tags ilike '%RTD%'
            and inventory_Winona_Rozelle > 0
        	and (
            reorder_point_Winona_Rozelle = 0
            or
            restock_level_Winona_Rozelle = 0
            OR
            reorder_point_Winona_Rozelle is null
            or
            restock_level_Winona_Rozelle is null

            )
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        select
            name,
            reorder_point_Winona_Rozelle,
            restock_level_Winona_Rozelle,
        from
            wh.mart.mart_item_curr_active
        where
            tags ilike '%RTD%'
        	and inventory_Winona_Rozelle > 0
            ;
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        select
            name,
            reorder_point_Winona_Rozelle,
            restock_level_Winona_Rozelle,
            product_category,
            tags
        from
            wh.mart.mart_item_curr_active
        where
            name ilike '%tiny%'
            ;
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        select
            name,
            reorder_point_Winona_Rozelle,
            restock_level_Winona_Rozelle,
            tags
        from
            wh.mart.mart_item_curr_active
        where
        	inventory_Winona_Rozelle > 0
        AND
            product_category ilike '%non alcoholic%'
            ;
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
