# README

A DBT project to convert the sales history dump to a data warehouse.

Outcomes:

- normalise sales history data
- provide sensible data marts

## TODO

- [ ] normalise:
  - [ ] normalise raw_sale
  - [ ] normalise raw_sale_line
  - [ ] normalise raw_payment.
  - [ ] add tests to stg tables.
- [ ] identify some useful marts

## Normalising

Normalising will require handling duplications. How to identify? If we ignore the file export dimension then receipt_number is likely the id.

How to handle the file... stuff. Need to have an ETL component that seperates the file context from the data. The hierarchy.

table append_instance
table filename
table sales_history

but normalising sales history isnt possible at this time so no point yet unless we can identify the blocks of data. Sales lines make it awkward. probably best to normalise after seperating out.

Receipt numbers are duplicated over files.

Normalised by identifying a `transaction_id`, a hashed combination of date, register, receipt_number, \_user, and details. Probably overkill but..

Nope. That's not going to work. details are different between line types..

### Forming a unique id

soooooo. receipt number, register, user should provide a unique id when excluding duplicates across files.

The mystery continues.. some lines can have the same date, receipt_number, user, while being different transactions. The key discriminant is the payment time. Wonder how that would work for split payments..

Confirmed. Sometimes the receipt number didnt increment so the final identifier is the payment time which is unique

Except its not. There are multiple instances of simultaneously recorded payments for the same receipt_number.

I dont know what to do now, it should have been simple to deduplicate. We're gna have to form transaction_file_id based on the local ordered appearance of transactions based on the individual blocks then compare the Sale details lines of the blocks to deduplicate, selecting the first of any grouping.

Best confirm that's the best approach. Probably gna be a pandas job or something like that. Postgres doesnt support ffill/backfill.

Will need to add a export_row_order column and the transaction id idx.

adding the file_transaction_id has been fine but it still doesnt solve the problem of absolute id. maybe we get a sales table transaction id and join back.

Also could possibly write a window function. The transactoin id needs to be in the initial read though or the order is lost. unless we add a row number$$

### A way forward

Have shown that date, details and receipt number are sufficient to form unique sale ids. Now to execute in dbt models. First create the id and drop the duplicates then break out into separate tables.

### Procedure

1. number rows by insert order
2. generate rowwise_sale_id based on the sales mask based cumsum based on insert order
3. generate the sale id from the hashed combination of date, details and receipt number
4. join sale_id on the rowwise_sale_id
5. select distinct sale_id, line_type.

Numbering rows by insert order is the problem. Will have to be done in the CLI app.
