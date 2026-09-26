# Adding Python packages to an EMR cluster

A2 does not need this: `stage2_spark.py` imports only what EMR's Spark release already has.
This example is for when a job needs a package the release lacks, or a newer version of one.

## Where Python runs

Stage 1 has no cluster. It is one EC2 box, and `uv run --with …` installs on the only machine
there is.

On EMR, a step submitted with `--deploy-mode client` runs its driver (the top level of your
script) on the primary node. The code inside `mapPartitions`, a UDF or `applyInPandas` runs in
executors on the core nodes, and each executor imports from its own node's Python. A package
has to be on every node that runs your code, and Spark has to be told which Python to use.

## The files

- `bootstrap-uv.sh` is a bootstrap action: a script EMR runs on each node while the cluster
  starts. It installs uv and builds `/opt/a2-venv` with newer pandas and duckdb on every node,
  and a second copy, `/opt/a2-driver-only`, on the primary node only.
- `whoami_job.py` prints which Python, pandas and duckdb the driver and each executor see, then
  runs a duckdb query inside the executors.
- `test-on-emr.sh` launches a 1 + 2 node cluster with the bootstrap action, submits the job
  three ways and runs `duckdb_in_spark.py`, prints what each saw, and deletes everything on exit.
  Run it in CloudShell, which is already signed in as you: clone the repo there and run
  `bash test-on-emr.sh m7g.xlarge`. It needs the EMR roles from A2's "Once per account" block and
  costs a few cents.

## What it showed (m8g.xlarge, emr-spark-8.0.0, 2026-09-28, two accounts)

| Submission | Driver | Executors | duckdb inside executors |
|---|---|---|---|
| default | EMR's Python 3.11, pandas 3.0.2, no duckdb | the same | fails: `No module named 'duckdb'` |
| `--conf spark.pyspark.driver.python=/opt/a2-driver-only/bin/python` | the venv: pandas 3.0.6, duckdb 1.5.5 | still EMR's Python: pandas 3.0.2, no duckdb | fails, same error |
| `--conf spark.pyspark.python=/opt/a2-venv/bin/python` and `…driver.python=…` | the venv | the venv: pandas 3.0.6, duckdb 1.5.5 | works |

The cluster with the bootstrap action reached `WAITING` in 191 s on one account and 346 s on
another; clusters without one took 194 to 255 s, so one launch per setup does not separate the
bootstrap from ordinary launch variance. uv built the environments on arm64 with nothing
architecture-specific in the script.

The same effect on Glue comes from the job argument `--additional-python-modules`.

## DuckDB inside Spark: `duckdb_in_spark.py`

Spark still splits the data, schedules the tasks and runs the shuffle. DuckDB runs inside one
task's Python worker, on whatever chunk Spark hands it. The same aggregation five ways, 40 million
cached rows, 1 + 2 m8g.xlarge nodes, 16 partitions, 4 cores per executor:

| Method | Wall | |
|---|---|---|
| A. Spark alone, `groupBy().agg()` | 1.2 s | stays in the JVM; Spark aggregates partially before the shuffle |
| B. pandas per group, `applyInPandas` | 4.1 s | every row crosses JVM → Arrow → Python |
| C. DuckDB per group | 2.3 s | the same crossing as B, a faster engine on each chunk |
| D. DuckDB per partition, Spark combines | 3.1 s | no better than A's own partial aggregation, and still pays the crossing |
| E. everything to the driver, DuckDB there | 1.8 s | one machine does all the work; fine only while the data fits in the driver |

All five produced the same 265 zones and 40,000,000 rows. DuckDB reported 4 threads by default
inside one task on a 4-vCPU node where Spark runs 4 tasks at once, so method C ran 16 DuckDB
threads on 4 cores; method D sets `threads = 1`. DuckDB's memory also sits outside Spark's
accounting, so a large chunk can get the container killed.

## pandas 3

EMR's Python has pandas 3.0.2, and the bootstrap installs 3.0.6 cleanly. Open-source PySpark
4.2.0 warns that it does not yet fully support pandas 3, which is why A2 pins `pandas<3` on the
laptop and on the Stage 1 box; EMR's Spark build printed no such warning in any step.
