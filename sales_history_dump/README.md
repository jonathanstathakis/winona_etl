# README

A tool for integrating manually downloaded sales history into a locally stored data lake. Moves all files matching "sales\_\*.csv" (via glob) from local `~/Downloads` to a dir..

Use the command line interfaace from within the dir `uv run sales_history_dump` to execute. Must provide databse name, database username, as well as source path and destination path for the csv files. use `uv run sales_history_dump` for more info. Currently rebuilds the the table from scratch on every execution, so as to stay in sync with the contents of the `destination_dir` files.

## TODO

- [ ] separate file movement form insertion logic
- [ ] modify data insertion to append rather than create new table each time.
- [ ] add latest transaction date metric to output

## Development

See <https://packaging.python.org/en/latest/guides/creating-command-line-tools/> for project structure.
