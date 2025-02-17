# tpch\_runner - TPC-H Benchmark Tool

tpch\_runner is a database-agnostic TPC-H benchmark tool designed for easy testing, execution, and analysis of TPC-H queries across multiple databases.

## Features

- **CLI-driven TPC-H benchmarking**:
  - Manage database connections.
  - Generate test data.
  - Prepare databases (table creation, optimization, data loading, truncation, and reloading).
  - Execute individual queries or full TPC-H Powertest.
  - Manage and analyze test results through result review, validation, comparison and other provided features.
  - Generate comparison charts for Powertest results.
  - Bundle a small dataset which can be used to verify database setup, TPC-H compliance.
- **Multi-database support**:
  - Built-in support for MySQL, PostgreSQL, DuckDB, and RapidsDB
  - Open architecture for easy integration of additional databases

## Installation

Installation is straightforward, just clone to local and do a pip edit mode installation.

```sh
$ git clone https://github.com/your-repo/tpch_runner.git
$ cd tpch_runner
$ pip install -e .
```

### Important Notes

- tpch_runner can be used to generate TPC-H test data but does not include **TPC-H dbgen and qgen**. All you need for this feature is copy the compiled version of below files into `tpch_runner/tpch/tool`:
  - `dbgen`
  - `qgen`
  - `dists.dss`
- **Handling Line Delimiters**:
  - Official TPC-H `dbgen` generates data files with `|\n` delimiters, which some databases (e.g., PostgreSQL) do not support. You have to remove the line delimiters before data files at first or go for next method.
  - To avoid this issue, consider using [this TPC-H dbgen variant](https://github.com/gregrahn/tpch-kit) that generates data files without delimiters.

## Getting Started

tpch_runner provides a command line (CLI) tool `runner` for all features. Use `-h` or `--help` for detailed context related help information.

```sh
$ runner -h
Usage: runner [OPTIONS] COMMAND [ARGS]...
```

A TPC-H benchmark test basically follows below process with steps that can be done with `runner` tool:

1. Setup database connection
2. Prepare TPC-H database (create tables, generate data, load data, optimize)
3. Test run individual queries
4. Conduct TPC-H Powertest
5. Analyze results
6. Compare test results

All above can be done with `runner`:

```sh
# Add a Database Connection
$ runner db add -H localhost -t mysql -u root -W -d tpch -a mysql2 -p 3306
Enter database password:
[INFO] Added database connection.

# create tables
$ runner db create -a my1

# Load Data
$ runner db load -a duck -m ','

# Test run individual query Q15 without saving test result
$ runner run query -a duck 15 --no-report

# Run a TPC-H Powertest on duckdb
$ runner run powertest -a duck

# Result Analysis
$ runner power list
+------+----------+---------------------+-----------+---------------+---------+
|   ID | DB       | Date                | Success   |   Runtime (s) | Scale   |
|------+----------+---------------------+-----------+---------------+---------|
|    2 | mysql    | 2025-01-19 21:48:23 | True      |        0.0492 | small   |
|   10 | rapidsdb | 2025-01-27 19:44:22 | True      |        5.5694 | small   |
|   11 | rapidsdb | 2025-01-27 20:14:25 | True      |       15.7367 | 1       |
|   13 | mysql    | 2025-01-27 21:30:48 | True      |      542.035  | 1       |
|   16 | pg       | 2025-01-27 21:52:47 | True      |        9.3369 | 1       |
|   17 | duckdb   | 2025-01-30 20:21:50 | True      |        0.8701 | small   |
|   20 | pg       | 2025-01-30 23:53:07 | True      |       14.8139 | 1       |
+------+----------+---------------------+-----------+---------------+---------+

# show a powertest test result
$ runner power show 18 | more

Powertest Details:
+---------------+--------------------+
| Attribute     | Value              |
|---------------+--------------------|
| ID            | 18                 |
| Database      | pg                 |
| Scale         | small              |
| Test Time     | 2025-01-30         |
| Success       | True               |
| Runtime (s)   | 12.533000000000001 |
| Result Folder | pg_20250130_215210 |
+---------------+--------------------+
...

# validate test results to known good answer
$ runner power validate 18

# compare test results between different or same databases
$  runner power compare -s 11 -d 20

Powertest Result Comparison:
+---------------+--------------------------+--------------------+
| Attribute     | Source                   | Destination        |
|---------------+--------------------------+--------------------|
| Database      | rapidsdb                 | pg                 |
| Scale         | 1                        | 1                  |
| Success       | True                     | True               |
| Runtime (s)   | 15.7367                  | 14.8139            |
| Result Folder | rapidsdb_20250127_201425 | pg_20250130_235307 |
+---------------+--------------------------+--------------------+

# individual query test result side by side comparison followed
...

# generate charts for multiple Powertest runtime comparison
$ runner power multi 2 16 18
```

## Supported Databases

- MySQL
- PostgreSQL
- RapidsDB
- DuckDB

Adding other databases is a  fairly trivial process.

---

For more details, refer to the full documentation or run `runner -h` for CLI usage guidance.
