"""Print which Python, pandas and duckdb the driver and the executors see, then run duckdb
inside the executors. Submit it as an EMR step three ways (see README.md) to see where a
package has to be installed for Spark to use it."""
import importlib, json, platform, sys

from pyspark.sql import SparkSession, functions as F


def seen():
    out = {"host": platform.node(), "arch": platform.machine(),
           "python": f"{sys.executable} {platform.python_version()}"}
    for name in ("pandas", "duckdb", "pyarrow"):
        try:
            out[name] = importlib.import_module(name).__version__
        except Exception as e:
            out[name] = f"missing ({type(e).__name__})"
    return out


spark = (SparkSession.builder.appName("whoami")
         .config("spark.ui.showConsoleProgress", "false").getOrCreate())
print("DRIVER", json.dumps(seen()), flush=True)

try:
    views = (spark.sparkContext.parallelize(range(16), 16)
             .mapPartitions(lambda _: [json.dumps(seen(), sort_keys=True)])
             .distinct().collect())
    for v in sorted(views):
        print("EXECUTOR", v, flush=True)
except Exception as e:
    print("EXECUTOR_FAILED", type(e).__name__, str(e).splitlines()[0][:300], flush=True)


def count_with_duckdb(pdf):
    import duckdb                                       # imported inside the task, on the executor
    return duckdb.sql("SELECT any_value(g) AS g, count(*) AS n FROM pdf").df()


try:
    rows = (spark.range(0, 1_000_000).withColumn("g", F.col("id") % 4)
            .groupBy("g").applyInPandas(count_with_duckdb, "g long, n long")
            .orderBy("g").collect())
    print("DUCKDB_IN_EXECUTORS ok", [(r.g, r.n) for r in rows], flush=True)
except Exception as e:
    print("DUCKDB_IN_EXECUTORS failed", type(e).__name__, str(e).splitlines()[0][:300], flush=True)

spark.stop()
