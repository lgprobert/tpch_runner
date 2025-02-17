# tpch_runner - TPC-H benchmark tool

Created on: 2025/1/9

tpch_runner is a database neural TPC-H benchmark tool.

## Major features
- CLI driven TPC-H test activities:
  - manage database connections
  - test data generation
  - test preparation including table creation, optimization, data loading, truncation and reloading
  - run individual query and/or Powertest
  - manage powertest results
  - result analysis by compare powertest results, generating cpmarison charts
- Built-in multiple database support (MySQL, PostgreSQL, Duckdb, and RapidsDB) and open architecture that allows many to be added.

## Installation
Assume you run the tool on Linux.
You can clone the repository or directly pip install.

1. Clone or download the repository to local, then conduct edit more PIP installation.
```sh
$ cd tpch_runner
$ pip install -e .
```

2. pip install
```sh
$ pip install tpch_runner
```

> Note:
   - For license concern, TPC-H dbgen and qgen tools are not included in this repository. To use **data generation** feature, download TPC-H toolkit and make the tool by yourself, then place below three files into `tpch_runner/tpch/tool` directory:
      - dbgen
      - qgen
      - dists.dss
   - Data files made by official TPC-H dbgen carry line delimiter `'|\n'`. The line delimiters must be stripped before the data files can be used by PostgreSQL and other databases do not support line delimiter. You can consider to use an variant of TPC-H dbgen from `https://github.com/gregrahn/tpch-kit` that generates data files without line delimiter.

## Configurations
Before start to use the tool, configure `tpch_runner/config.py` or just let the tool run with default configurations.

## Usage
The tool is used through the CLI tool `runner`, combine with `-h` or `--help` command line option for context help.

```sh
$ runner -h
Usage: runner [OPTIONS] COMMAND [ARGS]...

 Rapids Installer CLI tool

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────╮
│         -v    Set for verbose output                                                                       │
│ --help  -h    Show this message and exit.                                                                  │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────────╮
│ db                    Manage database server connections.                                                  │
│ generate              Generate TPC-H test data set.                                                        │
│ power                 Manage Powertest results.                                                            │
│ result                Manage test results.                                                                 │
│ run                   Manage test results.                                                                 │
│ version               Show Transformer version information.                                                │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

# to know how to add a database connection, use `-h` on applicable command
$ runner db add -h

 Usage: runner db add [OPTIONS]

 Add a new database connection.

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --type      -t  [mysql|pg|rapidsdb|duckdb]  DB type                                                        │
│ --host      -H  TEXT                        Database host                                                  │
│ --port      -p  TEXT                        Database port                                                  │
│ --user      -u  TEXT                        Database user                                                  │
│ --password  -W  TEXT                        Database password                                              │
│ --db        -d  TEXT                        Database name                                                  │
│ --alias     -a  TEXT                        Database alias                                                 │
│ --help      -h                              Show this message and exit.                                    │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

```

### Manage database connection
```sh
# add a MySQL database connection, `-a` (connection alias) must be unique
$ runner db add -H localhost -t mysql -u root -W -d tpch -a mysql2 -p 3306
Enter database password:
[INFO] Added database connection.
Database added successfully.

# list configured database connections
$ runner db list
+----+----------+-----------+-------------------------------+--------+----------+---------+
|    | Type     | Host      | Port                          | User   | DBName   | Alias   |
|----+----------+-----------+-------------------------------+--------+----------+---------|
|  1 | pg       | localhost | 5432                          | rapids | tpch     | pg1     |
|  2 | mysql    | localhost | 3306                          | root   | tpch     | my1     |
|  3 | rapidsdb | asr1      | 4333                          | RAPIDS | moxe     | rdp     |
|  4 | duckdb   | localhost | /home/robert/data/tpch_runner |        |          | duck    |
|  5 | mysql    | localhost | 3306                          | root   | tpch     | mysql2  |
|  6 | mysql    | localhost | 3306                          | root   | tpch     | mysql3  |
+----+----------+-----------+-------------------------------+--------+----------+---------+

# remove a database connection
$ runner db delete 6
[INFO] Deleted database 6.
Database deleted successfully.
```

### Prepare database
runner can help to prepare the database. runner also measures and shows execution time for major operations.
```sh
# drop all tables from database, use `-a` to specify database target
$ runner db drop -a duck
[INFO] Table all are dropped.
0.0210 seconds

# create tables
$ runner db create -a duck
TPC-H tables are created.
0.0105 seconds.

# load data
# tip: use `-t` to specify individual table, omit for 'all'.
# tip: use `-m` to specify data directory.
# tip: use `--no-optimize` to disable optimization meatures run before and after loading.
$ runner db load -a duck -m ','
[INFO] Running before load optimization.
table: region
table: nation
table: customer
table: part
table: supplier
table: partsupp
table: orders
table: lineitem
0.0648 seconds.
[INFO] Running after load optimization.
```

Database specific optimizations can be run at `runner db load` and `runner db reload` through the `--optimize` and `--no-optimize`, this is very handy when truncate and reload all tables or just reload a single small table.

### Run TPC-H query
Use `runner run` command to run an individual TPC-H query or TPC-H Powertest test.
When you seriously run a complicated query, you will want to save the test result for later analysis. But if you just do a test run, you probably just want the execution time instead of saving a full result. In this case, use `--no-result` to specify running a query or Powertest without saving result.
```sh
# Run TPC-H Q15 query on Duckdb database
$ runner run query -a duck 15

Q15 succeeds, return 1 rows.
0.0142 seconds.
[INFO] Test result added: q15 on duckdb.
Query 15 executed successfully, row count: 1.
Result saved to duckdb_q15_2025-02-09-204804.csv.

-------------------------------------------------------

Query Result:
+-------------+----------+-------------+-----------+-----------------+
|   s_suppkey | s_name   | s_address   |   s_phone |   total_revenue |
|-------------+----------+-------------+-----------+-----------------|
|           3 | TruValue | Gough       |       111 |          178534 |
+-------------+----------+-------------+-----------+-----------------+
```
After a query is finished run, `runner` will shows following information:
- Whether the query run is succeeded,
- The returned row count,
- The execution time,
- If `--report` option (default) is chosen, the report file name,
- The query result will always be printed on stdout.

### Run TPC-H Powertest
TPC-H Powertest can be easily run with `runner run powertest` command by specifying target database alias. Like running individual query, we can decide if we want to keep the test result files by `--report/--no-report` option.

If we decide to keep test results, a test folder with unique name will be created and results of each query is saved to a result file with query index as name, for example for Q1, the result file will be <test_folder>/1.txt.

TPC-H Powertest is run per TPC-H specification in below order:
```python
[14, 2, 9, 20, 6, 17, 18, 8, 21, 13, 3, 22, 16, 4, 11, 15, 1, 10, 19, 5, 7, 12]
```

## Result Analysis
tpch_runn can optionally store query execution results along with other critical test information to support test result analysis. When we choose to save a query or Powertest test result, below information will be stored:
- database type
- test run succeed or not
- resultset rowcount
- result file name and powertest result folder for powertest
- query name
- run time

There can be four possible results when running a query:
- succedds with correct results
- succeeds but results are incorrect
- finishes but no results
- fails

Test result analysis so include following features:
- Validate result agains answers
- List results to compare
- Compare two query or Powertest results
- Show detailed test result data
- Generate Powertest result charts (line chart or bar chart)
- Generate Powertest charts for multi-results comparison
