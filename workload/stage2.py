"""Stages 2a, 2b and 3, the work only: the Spark job, phase A then phase B.

For reading. The graded runs use stages/stage2_spark.py, which does exactly this and adds the timers and
the metrics blocks. EMR (Stages 2a and 2b) and Glue (Stage 3) run the same file.

Input:  matrix.parquet (Stage 1's output) and zones.parquet (265 rows, the zone lookup)
Output: features/ in S3, one row per pickup zone. Phase B's scores are computed and not kept.
"""
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .config("spark.sql.shuffle.partitions", "16")                      # 16 chunks after the shuffle
         .config("spark.sql.adaptive.coalescePartitions.enabled", "false")  # and they stay 16
         .getOrCreate())

trips = spark.read.parquet("s3://<your bucket>/a2/matrix.parquet")
zones = spark.read.parquet("s3://<your bucket>/a2/raw/zones.parquet")


# ---- Phase A: join, then one pandas function per pickup zone -----------------------------------------

def zone_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Called once per zone, with every trip of that zone in one pandas DataFrame."""
    tip_ratio = (pdf["tip_amount"] / pdf["fare_amount"].clip(lower=0.01)).to_numpy()
    return pd.DataFrame([{
        "PULocationID": int(pdf["PULocationID"].iloc[0]),
        "n_trips": len(pdf),
        "mean_fare": float(pdf["fare_amount"].mean()),
        "mean_tip_ratio": float(np.mean(tip_ratio)),
        "p90_distance": float(np.percentile(pdf["trip_distance"], 90)),
        "fare_per_km": float((pdf["fare_amount"] / pdf["trip_distance"].clip(lower=0.1)).median()),
    }])

joined = trips.join(F.broadcast(zones),                 # the 265-row lookup is copied to every executor
                    trips.PULocationID == zones.LocationID, "left")
features = joined.groupby("PULocationID").applyInPandas( # every row goes into the shuffle, so that
    zone_features,                                        # each zone's rows meet in one place
    schema="PULocationID int, n_trips long, mean_fare double, mean_tip_ratio double, "
           "p90_distance double, fare_per_km double")
features.write.mode("overwrite").parquet("s3://<your bucket>/a2/stage2-2n/features")


# ---- Phase B: 60 independent model fits (12 settings x 5 folds) -----------------------------------------

# a 200,000-row sample of the trips with the zone features joined on, collected to the driver
# and broadcast to every executor as numpy arrays X and y
features = spark.read.parquet("s3://<your bucket>/a2/stage2-2n/features")
sample = (trips.select("PULocationID", "trip_distance", "passenger_count", "fare_amount")
               .sample(fraction=200_000 / trips.count(), seed=436)
               .join(features, "PULocationID").toPandas().dropna())
cols = ["trip_distance", "passenger_count", "mean_fare", "mean_tip_ratio", "p90_distance", "fare_per_km"]
X = np.column_stack([np.ones(len(sample))] + [sample[c].to_numpy(float) for c in cols])
y = sample["fare_amount"].to_numpy(float)
data = spark.sparkContext.broadcast((X, y))

def fit_one(setting, fold, rounds, folds=5):
    """One fit: hold out one fold, repeat a least-squares fit `rounds` times, score the held-out fold.
    Pure numpy arithmetic, no data movement: each fit is an independent lump of CPU work."""
    X, y = data.value
    train = (np.arange(len(y)) % folds) != fold
    Xt, yt = X[train], y[train]
    pred = np.zeros_like(yt); resid = yt.copy()
    for _ in range(rounds):
        w, *_ = np.linalg.lstsq(Xt, resid, rcond=None)
        pred += 0.5 * (Xt @ w); resid = yt - pred
    return float(np.mean((X[~train] @ w * 0.5 + pred.mean() - y[~train]) ** 2))

grid = [(s, f) for s in range(12) for f in range(5)]                    # 60 fits
scores = (spark.sparkContext.parallelize(grid, len(grid))               # one task per fit
            .map(lambda sf: fit_one(sf[0], sf[1], rounds=50 * (1 + sf[0])))  # later settings run longer
            .collect())
