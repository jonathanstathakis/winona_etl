# README

A tool for integrating manually downloaded sales history into a locally stored data lake. Moves all files matching "sales\_\*.csv" (via glob) from local `~/Downloads` to a dir..

Use the command line interfaace from within the dir `uv run sales_history_dump` to execute. Must provide databse name, database username, as well as source path and destination path for the csv files. use `uv run sales_history_dump` for more info. Currently rebuilds the the table from scratch on every execution, so as to stay in sync with the contents of the `destination_dir` files.

## Inventory Data

The inventory data is not historical, only ever now. It could be useful to store the historical data. Therefore we'll add the download
date to the processed files before storage then ingest into the database. This handles append logic nicely. The only thing is that we'll
need to do the first step in python, or at least it'll be easier to do so..

## TODO

### Sales History

- [ ] modify data insertion to append rather than create new table each time. This is necessary for function because the dbt module is referencing the table directly so cannot drop existing table, therefore cannot add any more sales history.
- [ ] separate file movement form insertion logic
- [ ] add latest transaction date metric to output

### Inventory

- [x] define flow from Downloads to storage to ingestion

## Development

See <https://packaging.python.org/en/latest/guides/creating-command-line-tools/> for project structure.
