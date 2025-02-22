# tpch_runner - TPC-H benchmark run tool

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

╭─ Options ─────────────────────────────────────────────────────────────────────────────────╮
│         -v    Set for verbose output                                                      │
│ --help  -h    Show this message and exit.                                                 │
╰───────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ────────────────────────────────────────────────────────────────────────────────╮
│ db                    Manage database server connections.                                 │
│ generate              Generate TPC-H test data set.                                       │
│ power                 Manage Powertest results.                                           │
│ result                Manage test results.                                                │
│ run                   Manage test results.                                                │
│ version               Show Transformer version information.                               │
╰───────────────────────────────────────────────────────────────────────────────────────────╯

# to know how to add a database connection, use `-h` on applicable command
$ runner db add -h

 Usage: runner db add [OPTIONS]

 Add a new database connection.

╭─ Options ─────────────────────────────────────────────────────────────────────────────────╮
│ --type      -t  [mysql|pg|rapidsdb|duckdb]  DB type                                       │
│ --host      -H  TEXT                        Database host                                 │
│ --port      -p  TEXT                        Database port                                 │
│ --user      -u  TEXT                        Database user                                 │
│ --password  -W  TEXT                        Database password                             │
│ --db        -d  TEXT                        Database name                                 │
│ --alias     -a  TEXT                        Database alias                                │
│ --help      -h                              Show this message and exit.                   │
╰───────────────────────────────────────────────────────────────────────────────────────────╯
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

### Saving test results

tpch_runner can optionally store query execution results along with other critical test information to support test result analysis. 

Both sub-commands (`query` and `powertest`) of `runner run` provides command line options: `--report` and `--no-report` to tell if the test result should be saved.

When a result is to be saved, below information will be included:

- database type
- test run succeed or not
- result set rowcount
- result file name and result folder for Powertest 
- query name
- run time

> Note: `--report` option is chosen by default. Specify `--no-report` to run a query or a Powertest without saving result data.

## Result Analysis

tpch_runner provides multiple ways to help users analyze the TPC-H benchmark test results, along with individual TPC-H query results. 

- Manage test results including Powertest results and individual query results
- Show details of a test result
- Validate result against answers
- Compare two results
- Generate charts  (line chart or bar chart) for Powertest results
- Generate multi-results comparison charts for Powertest results

### Basic Result Analysis

The biggest thing most people concern most when doing TPC-H benchmark tests is **query execution time** and **total execution time**. With these data for the database we test and results of the other databases performed, we would know how fast the current database could be, or host fast of one database configuration compares to other configurations of same database. 

But besides these execution time, there are other equally important data of TPC-H test we should care, which are whether the TPC-H test passes all queries and produces correct answers.

Running A TPC-H query or Powertest can lead to four possible results:
- succeeds with correct results
- succeeds but results are incorrect
- finishes but no results
- fails

It is easy to know a query is failed if it fails. But it would be more difficult if a query finishes without errors, or even produces results. In that case, the best method to validate test result is compare results with known good results, or compare results to results of other databases running same queries under same data scale.

tpch_runner provides the various methods to achieve these goals.

#### Manage test results

```sh
# list query results
+------------+---------+--------+---------+-------------+-----------+- --------+------------+
|   Power ID | Test ID | DB     | Query   | Test Time   | Success   | Rowcount | Runtime(s) |
|------------+---------+--------+---------+-------------+-----------+----------+------------|
|          2 |      23 | mysql  | q14     | 2025-01-20  | True      |        1 |     0.0026 |
|          2 |      24 | mysql  | q2      | 2025-01-20  | True      |        1 |     0.0011 |
|          2 |      25 | mysql  | q9      | 2025-01-20  | True      |        6 |     0.0009 |
|          2 |      26 | mysql  | q20     | 2025-01-20  | True      |        1 |     0.0015 |
|          2 |      27 | mysql  | q6      | 2025-01-20  | True      |        1 |     0.0008 |
|          2 |      28 | mysql  | q17     | 2025-01-20  | True      |        1 |     0.0011 |
...

# list individual query results (non powertest results)
$ runner result list --single
+-----------+--------+---------+-------------+-----------+------------+---------------+
|   Test ID | DB     | Query   | Test Time   | Success   |   Rowcount |   Runtime (s) |
|-----------+--------+---------+-------------+-----------+------------+---------------|
|       141 | pg     | q15     | 2025-01-28  | True      |          0 |        0.3442 |
|       142 | pg     | q15     | 2025-01-28  | True      |          0 |        0.3419 |
|       143 | pg     | q15     | 2025-01-28  | True      |          0 |        0.3496 |
|       144 | pg     | q15     | 2025-01-28  | True      |          0 |        0.3458 |
|       167 | duckdb | q15     | 2025-01-31  | True      |          1 |        0.0413 |
|       190 | pg     | q1      | 2025-01-31  | True      |          4 |        1.1325 |
|       191 | pg     | q1      | 2025-01-31  | True      |          4 |        0.4382 |
|       258 | duckdb | q1      | 2025-02-10  | True      |          9 |        0.0058 |
|       259 | duckdb | q15     | 2025-02-10  | True      |          1 |        0.0142 |
+-----------+--------+---------+-------------+-----------+------------+---------------+

# delete a test result
$ runner result delete 141
[INFO] Test result 141 deleted.

# compare query results
# any two results (not necessarily same query) can be compared
 runner result compare -s 271 -d 272
+-------------+----------+---------------+
| Attribute   | Source   | Destination   |
|-------------+----------+---------------|
| Database    | pg       | pg            |
| Query       | q22      | q16           |
| Success     | True     | True          |
| Rowcount    | 1        | 19            |
| Runtime (s) | 0.0068   | 0.0057        |
+-------------+----------+---------------+
```

#### Show individual query test result details

A test result include many detailed information that can be shown with command `runner result show <result_id>`:

- database type
- query name
- success or not
- result set rowcount
- execution time
- result file (in CSV format)

Along with above information, the command also shows below information:

- Query text
- Results (if query text and results are more than 25 rows, a pagination pause would be displayed)

```sh
$ runner result show 191

Test Result Detail:
+-------------+-----------------------------+
| Attribute   | Value                       |
|-------------+-----------------------------|
| ID          | 191                         |
| Database    | pg                          |
| Query Name  | q1                          |
| Test Time   | 2025-01-31 02:27:34         |
| Success     | True                        |
| Rowcount    | 4                           |
| Runtime (s) | 0.4382                      |
| Result CSV  | pg_q1_2025-01-30-212734.csv |
+-------------+-----------------------------+


Press Enter to continue...q
Query Q1 Text:
--------------------------------------------------
-- using default substitutions


select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;

Test Result Set:
--------------------------------------------------
l_returnflag    l_linestatus             sum_qty    sum_base_price    sum_disc_price    sum_charge    avg_qty    avg_price    avg_disc    count_order
--------------  --------------  ----------------  ----------------  ----------------  ------------  ---------  -----------  ----------  -------------
A               F                    3.77341e+07       5.65866e+10       5.37583e+10   5.59091e+10    25.522       38273.1   0.0499853        1478493
N               F               991417                 1.4875e+09        1.41308e+09   1.46965e+09    25.5165      38284.5   0.0500934          38854
N               O                    7.4476e+07        1.11702e+11       1.06118e+11   1.10367e+11    25.5022      38249.1   0.0499966        2920374
R               F                    3.77198e+07       5.6568e+10        5.37413e+10   5.58896e+10    25.5058      38250.9   0.0500094        1478870
```

### Powertest Result Analysis

Powertest is a term per TPC-H specification. It can be simply understood as a single run of all 22 queries TPC-H queries in specific sequence. 

Powertest test result behaves like a placeholder to hold all 22 individual query results from the same test run. Individual query results within same Powertest run should be managed as a single entity. So attempt to delete a Powertest query result will fail with below error:

```sh
$ runner result delete 281
Fails to delete test result 281.
Exception: Can't delete individual test result associated with powertest.
Delete powertest to delete the entire test set.
```

Besides this, the management of Powertest results are similar to query results that results can be listed, deleted, shown and compared.

```sh
# list powertest results
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
|   21 | pg       | 2025-02-11 17:18:40 | True      |        0.1721 | small   |
+------+----------+---------------------+-----------+---------------+---------+

# delete a powertest record
$ runner power delete 2
```

#### Edit Powertest result to add more descriptive details

Powertest record can be edited too, but only for two descriptive attributes:

- Scale factor (`-s`): the value can be **small** or an integer number where **small** is  the bundled small dataset.
- Comment (`-c`): Any further description about the Powertest, like setup, configuration or any information that help to describe the benchmark test in more details.

```sh
$ runner power update -s small 20 -c "postgresql small dataset"
[INFO] PowerTest 20 updated.
```

#### Show Powertest result

Showing Powertest result has two parts:

- The overall information of the test:
  - Database type
  - Data scale
  - Success or not 
  - total run time
  - Result folder 
- Overall result of each query

```sh
$ runner power show 20

Powertest Details:
+---------------+--------------------+
| Attribute     | Value              |
|---------------+--------------------|
| ID            | 20                 |
| Database      | pg                 |
| Scale         | small              |
| Test Time     | 2025-01-30         |
| Success       | True               |
| Runtime (s)   | 14.813899999999995 |
| Result Folder | pg_20250130_235307 |
+---------------+--------------------+

----------------------------------------------------------------------

Powertest Individual Query Results:
+------+---------+-----------+------------+---------------+--------------+
|   ID | Query   | Success   |   Rowcount |   Runtime (s) | Result CSV   |
|------+---------+-----------+------------+---------------+--------------|
|  252 | q1      | True      |          4 |        0.4301 | 1.csv        |
|  237 | q2      | True      |        100 |        1.4603 | 2.csv        |
|  246 | q3      | True      |         10 |        0.2775 | 3.csv        |
|  249 | q4      | True      |          5 |        0.175  | 4.csv        |
|  255 | q5      | True      |          5 |        0.402  | 5.csv        |
|  240 | q6      | True      |          1 |        1.1097 | 6.csv        |
|  256 | q7      | True      |          4 |        0.4347 | 7.csv        |
|  243 | q8      | True      |          2 |        0.7942 | 8.csv        |
|  238 | q9      | True      |        175 |        1.3869 | 9.csv        |
|  253 | q10     | True      |         20 |        0.2762 | 10.csv       |
|  250 | q11     | True      |       1048 |        0.1021 | 11.csv       |
|  257 | q12     | True      |          2 |        0.3066 | 12.csv       |
|  245 | q13     | True      |         42 |        1.1154 | 13.csv       |
|  236 | q14     | True      |          1 |        2.2862 | 14.csv       |
|  251 | q15     | True      |          1 |        0.3783 | 15.csv       |
|  248 | q16     | True      |      18314 |        0.1571 | 16.csv       |
|  241 | q17     | True      |          1 |        0.6025 | 17.csv       |
|  242 | q18     | True      |         57 |        1.4761 | 18.csv       |
|  254 | q19     | True      |          1 |        0.2815 | 19.csv       |
|  239 | q20     | True      |        186 |        0.9696 | 20.csv       |
|  244 | q21     | True      |        100 |        0.341  | 21.csv       |
|  247 | q22     | True      |          7 |        0.0509 | 22.csv       |
+------+---------+-----------+------------+---------------+--------------+
```

#### Compare two Powertest results

Similar to `show`, comparison between two Powertest results, despite whether they are multiple runs of same database or results of two different databases, falls into high level comparison and query to query comparison:

```sh
# compare RapidsDB to PostgreSQL of SF1 Powertest results
$ runner power compare -s 11 -d 20

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

----------------------------------------------------------------------

Powertest Individual Query Result Comparison:
+---------+--------------+-----------+--------------+------------+---------------+---------------+
| Query   | Success      | Success   |     Rowcount |   Rowcount |   Runtime (s) |   Runtime (s) |
|         | - rapidsdb   | - pg      |   - rapidsdb |       - pg |    - rapidsdb |          - pg |
|---------+--------------+-----------+--------------+------------+---------------+---------------|
| q14     | True         | True      |            1 |          1 |        0.4258 |        2.2862 |
| q2      | True         | True      |          100 |        100 |        0.5399 |        1.4603 |
| q9      | True         | True      |          175 |        175 |        0.7654 |        1.3869 |
| q20     | True         | True      |          186 |        186 |        0.9787 |        0.9696 |
...
```

> Note:
>
> - `compare` command currently does not tell if a result is correct or not, it merely lists the rowcount of each query. A test can be faster but with incorrect results.
> - The unequal rowcount of a query between two results are not highlighted, users need to carefully compare and find out any possible result discrepancy.

#### Validate a Powertest test result

Validation is important to make sure a TPC-H Powertest result is correct. tpch_runner develops methods to validate query result against correct answer. But the answer itself is dependent to the size of data scale because same query running in different test data size will have different results.

Since tpch_runner allows users running TPC-H benchmark test under any data scale, to the answer files for each specific data size will be user's responsibility to prepare. tpch_runner itself bundles the answer files for the bundled **small** data set. It is useful to use small dataset to quickly run through various features of tpch_runner, prepare database and ensure target database is ready for TPC-H benchmark test. 

To prepare answer files for a specific data scale, normally you need to run a few times Powertest of same data scale for one database or different databases, then compare test result from one to one. If the test result after cross comparison among 3 or 4 tests remain identical, there is high chance the test result is correct. 

The next step is make a test result as **answer**. This is a manual process and follow below procedure for this:

```sh
# switches to tpch_runner root
# 1. create answer directory, "scale_factor" is a number corresponding to data scale
$ mkdir tpch_runner/tpch/answer/<scale_factor>
# below command preapre the answer_dir for scale factor 10
$ mkdir tpch_runner/tpch/answer/10

# 2. copy answer files into the newly created answer_dir, file name must be an number
# and extension name should be 'csv', for Q1, the answer file is `1.csv`
cp <result_dir>/*.csv tpch_runner/tpch/answer/10/
```

> Note: 
>
> - `validate` sub-command conducts row by row, column by column comparison from result to answer. To use this, one set of answers must be prepared at first. 
> - Answers should be database neutral which means the answers should be able to validate results from different databases under same data scale.

#### Generate charts from Powertest results

```sh
# generate a barchart
$ runner power draw <powertest_id>

# generate a linechart for powertest 20
$ runner power draw -c line 20
Chart saved to /home/robert/data/tpch_runner/pg_20250130_235307.png
```

#### Generate comparison charts from multiple Powertest results

Using `runner power multi` command to generate comparison charts for multiple Powertest results. This command will result two chart files:

- `line-...-multi.png`: a line chart rendered query execution time for all Powertest results.
- `bar-...-multi.png`: a bar chart shows total execution time for all Powertest results.

```sh
$ runner power multi 10 16 20
Comparing test results of  rapidsdb_20250127_194422 pg_20250127_215247 pg_20250130_235307
Comparison charts are saved to /home/robert/data/tpch_runner/line-rapidsdb-pg-pg-multi.png, /home/robert/data/tpch_runner/bar-rapidsdb-pg-pg-multi.png.
```

![line_chart-multi](/home/robert/projects/tpch_runner/docs/imgs/line-rapidsdb-pg-pg-multi.png)

![barchart-multi](/home/robert/projects/tpch_runner/docs/imgs/bar-rapidsdb-pg-pg-multi.png.png)
