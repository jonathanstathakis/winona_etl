import marimo

__generated_with = "0.19.11"
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
    # Current snapshot of all active items
    mart_item_curr_active = mo.sql(
        """
        select * from wh.mart.mart_item_curr_active
        order by name
        """,
        engine=conn,
    )
    return (mart_item_curr_active,)

if __name__ == "__main__":
    app.run()