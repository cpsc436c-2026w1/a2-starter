# CPSC 436C 2026W1 Assignment 2 (draft)

The assignment: <https://cpsc436c-2026w1.github.io/a2-starter/>

This is a draft. The commands, the code and some details may still change before the release,
and the released version is the one that counts.

| File | What it is |
|---|---|
| `a2.qmd` | the assignment with every command in it, source; `docs/a2.html` is the rendered page |
| `workload/stage1.sql` | the query Stage 1 runs, for reading; no timers |
| `workload/stage2.py` | the Spark work of Stages 2a, 2b and 3, for reading; no metrics code |
| `stages/stage1_duckdb.py` | Stage 1: scan, filter, project to the model matrix |
| `stages/stage2_spark.py` | Stages 2a, 2b and 3: the Spark job, submitted to EMR and to Glue unchanged |
| `stages/parse_run_log.py` | turns a run's log into a `results.csv` row |
| `results.csv` | the results file, header only; keep it on your laptop |
| `examples/extra-packages/` | not part of A2: how to add Python packages to an EMR cluster with uv, and what DuckDB does inside a Spark job |

The data is not in this repository. The job aid's "Get the data" step fetches it into your bucket.
