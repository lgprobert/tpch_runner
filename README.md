# tpch\_runner: TPC-H Benchmark Tool

tpch\_runner is a database-agnostic TPC-H benchmark tool designed for running and analyzing the TPC-H benchmark across multiple databases. It allows easy setup, execution, and result analysis, make TPC-H testing of their database systems much more efficiently.

## Features

- **CLI-driven TPC-H benchmarking**:
  - Manage database connections.
  - Generate and load TPC-H test data.
  - Prepare databases (e.g., table creation, optimization, and data reloading).
  - Run individual queries or full TPC-H Powertests.
- **Comprehensive Result Analysis**:
  - Manage, validate, and compare benchmark results.
  - Generate charts to visualize Powertest results.
  - Bundle a small dataset for verifying database setup and TPC-H compliance.
- **Multi-database support**:
  - Built-in support for MySQL, PostgreSQL, DuckDB, and RapidsDB.
  - Open architecture to easily integrate additional databases.

## Installation

Getting started with **tpch_runner** is quick and simple. Just clone the repository and install it in editable mode:

```sh
# install by clone from github repository
git clone https://github.com/your-repo/tpch_runner.git
cd tpch_runner
pip install -e .

# install from Pypi package
pip install tpch_runner
```

### Important Notes

- **Test Data Generation**: **tpch_runner** supports TPC-H test data generation but **does not include dbgen or qgen**. You need to manually add the following compiled files to the `tpch_runner/tpch/tool` directory:
  - `dbgen`
  - `qgen`
  - `dists.dss`
- **Line Delimiters**:
  - The official TPC-H `dbgen` uses `|\n` line delimiters, which some databases (e.g., PostgreSQL) may not support. You can either remove these delimiters manually or use a [TPC-H dbgen variant](https://github.com/gregrahn/tpch-kit) that avoids them.

## Getting Started

To use **tpch_runner**, simply run the `runner` CLI tool. Use `-h` or `--help` for detailed help on any command:

```sh
$ runner -h
Usage: runner [OPTIONS] COMMAND [ARGS]...
```

### Typical Benchmark Workflow

1. Set up a database connection.
2. Prepare the TPC-H database (create tables, generate and load data, optimize).
3. Run individual queries or a full TPC-H Powertest.
4. Analyze benchmark results.
5. Compare results across different runs or databases.

### Example Commands

- **Add a Database Connection**:

```sh
$ runner db add -H localhost -t mysql -u root -W -d tpch -a mysql2 -p 3306
Enter database password:
[INFO] Added database connection.
```

- **Create Tables**:

```sh
runner db create -a my1
```

- **Load Data**:

```sh
runner db load -a duck -m ','
```

- **Run a Single Query:**

```sh
runner run query -a duck 15 --no-report
```

- **Run a TPC-H Powertest**

```sh
runner run powertest -a duck
```

- **Result Analysis:**

```sh
$ runner power list
+------+----------+---------------------+-----------+---------------+---------+
|   ID | DB       | Date                | Success   |   Runtime (s) | Scale   |
|------+----------+---------------------+-----------+---------------+---------|
|    2 | mysql    | 2025-01-19 21:48:23 | True      |        0.0492 | small   |
|   10 | rapidsdb | 2025-01-27 19:44:22 | True      |        5.5694 | small   |
|   17 | duckdb   | 2025-01-30 20:21:50 | True      |        0.8701 | small   |
|   20 | pg       | 2025-01-30 23:53:07 | True      |       14.8139 | 1       |
+------+----------+---------------------+-----------+---------------+---------+
```

- **Validate Test Results:**

```sh
runner power validate 18
```

- **Compare two test results:**

```sh
runner power compare -s 11 -d 20
```

- **Generate Comparison Charts:**

```sh
runner power multi 2 16 18
```

## Result Analysis

**tpch_runner** provides a variety of ways to analyze and visualize your benchmarking results:

- Manage Powertest and query results.
- View test result details.
- Validate results against known good answers.
- Compare results from different databases or test runs.
- Generate line and bar charts for visualizing Powertest performance.
- Create multi-result comparison charts.

![barchart-Powertest](./docs/imgs/duckdb_20250130_202150.png)

![linechart-multi-comparison](./docs/imgs/line-rapidsdb-pg-pg-multi.png)

## Supported Databases

- MySQL
- PostgreSQL
- RapidsDB
- DuckDB

Integrating additional databases is straightforward by **tpch_runner**'s open architecture.

---

For more details, refer to the documentation or run `runner -h` for CLI usage guidance.
