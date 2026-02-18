import marimo

__generated_with = "0.19.11"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        -- import all the file rows into one table
        -- normalises the column names
        -- includes the filename as a column
        -- adds a transaction id relatie to files based on appearance of line_type='Sale'.
        create table raw_sales_history as
        with
            raw_sales_history as (
                select
                    -- absolute table index
                    row_number() over () as idx,
                    -- mask used to generate a transaction id relative to files.
                    case
                        when line_type = 'Sale' then 1
                        else 0
                    end as sale_line_mask,
                    *
                from
                    read_csv(
                        '/Users/jonathan/jonathan/projects/winona/winona_data_wh/raw/*.csv',
                        sample_size = 10000,
                        normalize_names = true,
                        filename = true
                    )
            ),
            -- add a transaction id based on the sales line mask relative to files - unique to file, not absolute.
            fname_sale_pk as (
                select
                    sum(sale_line_mask) over (
                        partition by
                            filename
                        order by
                            idx
                    ) as file_transaction_id,
                    *
                from
                    raw_sales_history
            ),
            -- an abolute transaction id which ignores source files. 
            sale_pk as 
        (select
                    sum(sale_line_mask) over (
                        order by
                            idx
                    ) as sale_pk,
                    *
                from
                    fname_sale_pk
            ),

            file_row_idx as (
                select
                    row_number() over (
                        PARTITION by
                            filename
                        order by
                            idx
                    ) as file_row_idx,
                    *
                from
                    sale_pk
            ),
            filename_code as (
            select dense_rank() over (order by filename) as filename_code,
            *
            from
            	file_row_idx
            )
        select
            *
        from
            filename_code;

        select
            *
        from
            raw_sales_history
        order by
            idx
        limit
            10;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Now need to verify that some files will have cut-off transactions, i.e. not all transactions end with a Payment..
    """)
    return


@app.cell(hide_code=True)
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        -- get the last row of each file and return if line_type != 'Payment'

        with
            raw_sales_history as (
                select
                    *
                from
                    raw_sales_history
            ),
            last_row as (
            select
            	filename,
            	max(idx),
            	max(file_row_idx),
            	last(line_type order by idx)
        	from
            	raw_sales_history
            group by
            	filename
        	having
            	last(line_type order by idx) != 'Payment'
            )
        select
            *
        from
            last_row;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        selecta
            filename,
            * exclude filename
        from
            raw_sales_history
        where
            idx > 4040
        and
            idx < 4050
            ;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    1 file doesn't end with a payment. Which means we can't use the payment datetime for the id as was planned previously. What's going on with that one?
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Possibly transactions with attributes='onaccount' dont have payment lines.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Can't do any joins without ensuring referential integrity. I think some normalization is required after all. Need to investigate what columns such as attributes implies.
    """)
    return


@app.cell
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        select distinct attributes from raw_sales_history
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    5 attribute types: onaccount, delivery, null, pickup or layby. These are not all the same context.
    """)
    return


@app.cell(hide_code=True)
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        select distinct line_type, attributes from raw_sales_history
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    so attributes belongs to Sale.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Ok new plan - normalize first dedup later. Normalise requires a key to be shared between the tables. What is the key.
    """)
    return


@app.cell
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        select * from raw_sales_history order by idx limit 10;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    As we've seen
    """)
    return


@app.cell
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        create table receipt_dups_over_file as 
        with sale as (
        select
            *
        from
            raw_sales_history
        where
            line_type = 'Sale')
        select 
            filename,
            receipt_number,
            count(*) from sale group by filename, receipt_number having count(*) > 1;

        select * from receipt_dups_over_file limit 10;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    There are a number of duplicates, but what's going on there? The combination of receipt number and file should be unique.
    """)
    return


@app.cell
def _(mo, raw_sales_history, receipt_dups_over_file):
    _df = mo.sql(
        f"""
        with
            duplicate as (
                select
                    *
                from
                    receipt_dups_over_file
            ),
            raw_sales_history as (
                select
                    *
                from
                    raw_sales_history
            ),
            duplicate_filtered as (
                select
                    raw_sales_history.*
                from
                    duplicate
                    left join raw_sales_history on duplicate.filename = raw_sales_history.filename
                    and duplicate.receipt_number = raw_sales_history.receipt_number
            )
        select
            filename_code,
            receipt_number,
            date,
            line_type,
            customer_code,
            customer_name,
            note,
            details,
            * exclude (
            filename_code,
            receipt_number,
            date,
            line_type,
            customer_code,
            customer_name,
            note,
            details
            )
        from
            duplicate_filtered
        order by
            receipt_number,
            idx asc
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    We have the duplicates. What now? I think we normalise with file_transaction_id as key then analyse further. Theres too much noise and entanglement. What are they being normalised into? a file table, a transaction table, a sale table, sale line table, and payment table. files have many transactions, transactions have many sales, sale lines and payments.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Normalising
    """)
    return


@app.cell(hide_code=True)
def _(distinct_filename, mo, raw_sales_history):
    _df = mo.sql(
        f"""
        create table filename (pk int primary key, filename varchar unique);

        insert into
            filename (pk, filename)
        with
            distinct_filename as (
                select distinct
                    filename
                from
                    raw_sales_history
            )
        select
            row_number() over () as pk,
            filename
        from
            distinct_filename;
        """
    )
    return


@app.cell(hide_code=True)
def _(filename, mo):
    _df = mo.sql(
        f"""
        select * from filename limit 5;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    file_sale_pk is repeated over files. Need an absolute id.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Separate sale from file by a bridging table. Sale becomes 'pure' data with unfortunate repetitions.
    """)
    return


@app.cell
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        create table sale (
            pk int primary key,
            sale_id UINT64,
            date varchar,
            receipt_number varchar,
            customer_code varchar,
            customer_name varchar,
            note varchar,
            quantity varchar,
            subtotal varchar,
            sales_tax varchar,
            discount varchar,
            loyalty varchar,
            total varchar,
            details varchar,
            register varchar,
            _user varchar,
            status varchar,
            state varchar,
            attributes varchar
        );

        insert into sale (
            pk,
            sale_id,
            date,
            receipt_number,
            customer_code,
            customer_name,
            note,
            quantity,
            subtotal,
            sales_tax,
            discount,
            loyalty,
            total,
            details,
            register,
            _user,
            status,
            state,
            attributes
        )
        select
        	sale_pk as pk,
            hash(concat(date, details, receipt_number)) as sale_id,
            date,
            receipt_number,
            customer_code,
            customer_name,
            note,
            quantity,
            subtotal,
            sales_tax,
            discount,
            loyalty,
            total,
            details,
            register,
            _user,
            status,
            state,
            attributes,
        from
            raw_sales_history
        where
            line_type = 'Sale';
        select * from sale limit 5;
        """
    )
    return


@app.cell(hide_code=True)
def _(filename, filename_sale_join, mo, raw_sales_history):
    _df = mo.sql(
        f"""
        -- create a filename to sale bridge through sale_pk
        create table filename_to_sale (
            filename_pk int references filename (pk),
            sale_pk int references sale (pk)
        );

        insert into
            filename_to_sale (filename_pk, sale_pk)
        with
            filename as (
                select
                    *
                from
                    filename
            ),
            raw_sales_history as (
                select
                    *
                from
                    raw_sales_history
            ),
        filename_sale_join as (
        select
            filename.pk as filename_pk,
            sale_pk
        from
            filename
            left join raw_sales_history on filename.filename = raw_sales_history.filename
            )
        select 
            distinct filename_pk, sale_pk
            from filename_sale_join
            ;

        select
            *
        from
            filename_to_sale
        limit
            5;
        """
    )
    return


@app.cell
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        create table payment (
            pk int primary key,
            sale_pk int references sale (pk),
            date varchar,
            receipt_number varchar,
            paid varchar,
            details varchar
        );

        insert into
            payment (
                pk,
                sale_pk,
                date,
                receipt_number,
                paid,
                details
            )
        select
            row_number() over () as pk,
            sale_pk,
            date,
            receipt_number,
            paid,
            details
        from
            raw_sales_history
        where
            line_type = 'Payment';

        select * from payment limit 10;
        """
    )
    return


@app.cell
def _(mo, raw_sales_history):
    _df = mo.sql(
        f"""
        create table sale_line (
            sale_pk int references sale (pk),
            date varchar,
            receipt_number varchar,
            line_type varchar,
            note varchar,
            quantity varchar,
            subtotal varchar,
            sales_tax varchar,
            discount varchar,
            loyalty varchar,
            total varchar,
            details varchar,
            sku varchar,
            accountcodesale varchar
        );

        insert into
            sale_line (
                sale_pk,
                date,
                receipt_number,
                note,
                quantity,
                subtotal,
                sales_tax,
                discount,
                loyalty,
                total,
                details,
                sku,
                accountcodesale
            )
        with
            sale_line as (
                select
                    sale_pk,
                    date,
                    receipt_number,
                    note,
                    quantity,
                    subtotal,
                    sales_tax,
                    discount,
                    loyalty,
                    total,
                    details,
                    sku,
                    accountcodesale,
                from
                    raw_sales_history
                where
                    line_type = 'Sale Line'
            )
        select
            *
        from
            sale_line
        ;
        select * from sale_line limit 10;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    So now the tables are normalised. What now? Need to generate a unique sale identifier and find how many are duplicated.
    """)
    return


@app.cell
def _(filename_to_sale, mo, sale):
    _df = mo.sql(
        f"""
        with
            agged as (
            select
                count(*),
                sale_id,
                array_agg(date),
                array_agg(details),
            array_agg(filename_to_sale.filename_pk)
            from
            	sale
            left join
            	filename_to_sale
            on
            	sale.pk = filename_to_sale.sale_pk
            group by
                filename_pk, sale_id
            having
                count(*) > 1)
            -- select count(*) from sale_id;

        select * from agged limit 10;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    The combination of date, details and receipt_number results in unique rows when accounting for duplication over file. Receipt number had to be included because there were several sales whose date and details (the list of items) where duplicated but the receipt number differed.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Now to dedup. Best way to do it is to get the set of duplicated sales.
    """)
    return


@app.cell
def _(mo, sale):
    _df = mo.sql(
        f"""
        create table unique_sale as select distinct on (sale_id) * from sale;
        select * from unique_sale;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    now need to drop from payment and sale line and finally sale.
    """)
    return


@app.cell(hide_code=True)
def _(mo, payment, unique_sale):
    _df = mo.sql(
        f"""
        delete from payment using unique_sale where payment.sale_pk != unique_sale.pk;
        select * from payment;
        """
    )
    return


@app.cell
def _(mo, sale_line, unique_sale):
    _df = mo.sql(
        f"""
        delete from sale_line using unique_sale where sale_line.sale_pk != unique_sale.pk;
        select * from sale_line;
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Lessons Learned

    - Can get unique sale ids based on a combination of date, details, and receipt_number.
    - Breaking out the sale, sale_line and payment tables is straight foreward after forming the id.
    - There is probably more that can be normalised i.e. accounts, customers etc. Not worth it at this time.
    """)
    return


if __name__ == "__main__":
    app.run()
