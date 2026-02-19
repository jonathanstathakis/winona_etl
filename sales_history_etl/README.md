# README

A DBT project to convert the sales history dump to a data warehouse.

Outcomes:

- normalise sales history data
- provide sensible data marts

## TODO

- [x] normalise:
  - [x] normalise raw_sale
  - [x] normalise raw_sale_line
  - [x] normalise raw_payment.
- [x] identify some useful marts

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

Lightspeed does not provide a unique sale id, and presumably due to internal errors, receipt number is sometimes duplicated. We have generated a surrogate key from the hash of the date, details, and receipt_number fields.

## Useful Marts

What kind of marts would be useful? This will be an evolving question.Probably can't answer that right now. We could start with data health.

## Next Steps

Integrate inventory data in so we can start looking at categories, pricing and COG.

## Integrating Inventory

Should probably do it the same way the sales history is run. Difference with the sales history is that we want to record the export date because this will provide us with snapshots. Best to store the export date in the semi-processed file - each row gets an export date.