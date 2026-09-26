"""What DuckDB does, and does not do, inside a Spark job.

The same aggregation (rows and mean fare per pickup zone) five ways, on the same generated data:

  A  Spark alone            groupBy + agg: Spark plans the shuffle and aggregates in the JVM
  B  pandas per group       groupBy + applyInPandas: Spark shuffles, then hands each zone's rows
                            to a Python worker as one pandas DataFrame
  C  DuckDB per group       the same, with DuckDB doing the arithmetic on that DataFrame
  D  DuckDB per partition   mapInArrow: DuckDB pre-aggregates each partition where it already
                            sits, Spark shuffles only the small partial results and adds them up
  E  DuckDB on the driver   collect everything to the driver and let DuckDB do it there

Every method must produce the same totals. Prints one RESULT line per method.
"""
import os, time

from pyspark.sql import SparkSession, functions as F

N = int(os.environ.get("ROWS", 40_000_000))
PARTS = int(os.environ.get("PARTS", 16))

spark = (SparkSession.builder.appName("duckdb-in-spark")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", PARTS).getOrCreate())
sc = spark.sparkContext
print(f"SETUP rows={N} partitions={PARTS} executor_cores={sc.getConf().get('spark.executor.cores', '?')} "
      f"default_parallelism={sc.defaultParallelism}", flush=True)

df = (spark.range(0, N, numPartitions=PARTS)
      .select((F.col("id") % 265).cast("int").alias("zone"),
              ((F.col("id") * 7919 % 10000) / 100.0).alias("fare")))
df.cache().count()                                        # generate once; every method reads the same cache


def timed(name, fn):
    t0 = time.monotonic()
    try:
        rows = fn()
        total = sum(int(r[1]) for r in rows)
        print(f"RESULT {name} wall_s={time.monotonic() - t0:.1f} zones={len(rows)} rows_counted={total} "
              f"ok={total == N and len(rows) == 265}", flush=True)
    except Exception as e:
        print(f"RESULT {name} wall_s={time.monotonic() - t0:.1f} failed {type(e).__name__}: "
              f"{str(e).splitlines()[0][:200]}", flush=True)


# A. Spark alone
timed("A_spark", lambda: [(r.zone, r.n) for r in
      df.groupBy("zone").agg(F.count("*").alias("n"), F.avg("fare").alias("avg")).collect()])


# B. pandas on each zone's rows
def with_pandas(pdf):
    import pandas as pd
    return pd.DataFrame({"zone": [int(pdf.zone.iloc[0])], "n": [len(pdf)], "avg": [pdf.fare.mean()]})

timed("B_pandas_per_group", lambda: [(r.zone, r.n) for r in
      df.groupBy("zone").applyInPandas(with_pandas, "zone int, n long, avg double").collect()])


# C. DuckDB on each zone's rows
def with_duckdb(pdf):
    import duckdb
    return duckdb.sql("SELECT any_value(zone)::INT AS zone, count(*) AS n, avg(fare) AS avg FROM pdf").df()

timed("C_duckdb_per_group", lambda: [(r.zone, r.n) for r in
      df.groupBy("zone").applyInPandas(with_duckdb, "zone int, n long, avg double").collect()])


# D. DuckDB pre-aggregates each partition in place; Spark combines the partials
def partial_with_duckdb(batches):
    import duckdb, pyarrow as pa
    con = duckdb.connect()
    con.sql("SET threads = 1")                             # one Spark task already holds one core
    chunk = pa.Table.from_batches(list(batches))
    out = con.sql("SELECT zone, count(*) AS n, sum(fare) AS s FROM chunk GROUP BY zone").arrow()
    yield from (out.to_batches() if hasattr(out, "to_batches") else out.read_all().to_batches())

def two_level():
    partial = df.mapInArrow(partial_with_duckdb, "zone int, n long, s double")
    final = partial.groupBy("zone").agg(F.sum("n").alias("n"), (F.sum("s") / F.sum("n")).alias("avg"))
    return [(r.zone, r.n) for r in final.collect()]

timed("D_duckdb_per_partition", two_level)


# E. everything to the driver, DuckDB there
def on_driver():
    import duckdb
    everything = df.toArrow() if hasattr(df, "toArrow") else df.toPandas()
    return duckdb.sql("SELECT zone, count(*) AS n FROM everything GROUP BY zone").fetchall()

timed("E_duckdb_on_driver", on_driver)


# What DuckDB thinks it has, inside one executor task, when nobody tells it otherwise
def probe(_):
    import duckdb, os
    threads = duckdb.sql("SELECT current_setting('threads')").fetchone()[0]
    yield f"{os.cpu_count()} cores on the node, duckdb default threads={threads}"

print("THREADS", sc.parallelize([0], 1).mapPartitions(probe).collect()[0], flush=True)
spark.stop()
