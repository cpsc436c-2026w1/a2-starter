"""Deterministic taxi-shaped sample for the offline harness. Never used in the graded runs."""
import argparse, numpy as np, pandas as pd, pathlib

TLC_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
]

def make_sample(out_dir, n_rows=200_000, seed=436):
    rng = np.random.default_rng(seed)
    out = pathlib.Path(out_dir); (out / "trips").mkdir(parents=True, exist_ok=True)
    ranks = np.arange(1, 266)
    p = ranks ** -1.5; p /= p.sum()
    pu = rng.choice(ranks, size=n_rows, p=p)
    pickup = pd.Timestamp("2025-01-01") + pd.to_timedelta(
        rng.integers(0, 31 * 24 * 3600, n_rows), unit="s")
    dist = rng.gamma(2.0, 1.6, n_rows).round(2)
    fare = (3.0 + 2.5 * dist + rng.normal(0, 2, n_rows)).clip(2.5).round(2)
    tip = (fare * rng.beta(2, 8, n_rows)).round(2)
    df = pd.DataFrame({
        "VendorID": rng.integers(1, 3, n_rows).astype("int32"),
        # microsecond resolution: pandas/pyarrow default to nanosecond timestamps,
        # which Spark's parquet reader rejects outright (PARQUET_TYPE_ILLEGAL on
        # INT64 TIMESTAMP(NANOS)) — real TLC parquet is microsecond-resolution too.
        "tpep_pickup_datetime": pickup.astype("datetime64[us]"),
        "tpep_dropoff_datetime": (pickup + pd.to_timedelta(
            (dist * 240).astype(int), unit="s")).astype("datetime64[us]"),
        "passenger_count": rng.integers(1, 5, n_rows).astype("float64"),
        "trip_distance": dist,
        "RatecodeID": np.ones(n_rows, dtype="float64"),
        "store_and_fwd_flag": np.where(rng.random(n_rows) < 0.01, "Y", "N"),
        "PULocationID": pu.astype("int32"),
        "DOLocationID": rng.choice(ranks, size=n_rows, p=p).astype("int32"),
        "payment_type": rng.integers(1, 3, n_rows).astype("int64"),
        "fare_amount": fare, "extra": np.zeros(n_rows), "mta_tax": np.full(n_rows, 0.5),
        "tip_amount": tip, "tolls_amount": np.zeros(n_rows),
        "improvement_surcharge": np.full(n_rows, 0.3),
        "total_amount": (fare + tip + 0.8).round(2),
        "congestion_surcharge": np.full(n_rows, 2.5), "airport_fee": np.zeros(n_rows),
    })[TLC_COLUMNS]
    trips_path = str(out / "trips" / "part-0.parquet")
    df.to_parquet(trips_path, index=False)
    zones = pd.DataFrame({
        "LocationID": ranks.astype("int32"),
        "Borough": [f"B{r % 6}" for r in ranks],
        "Zone": [f"Zone {r}" for r in ranks],
    })
    zones_path = str(out / "zones.parquet")
    zones.to_parquet(zones_path, index=False)
    return {"trips_path": trips_path, "zones_path": zones_path, "n_rows": n_rows}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True); ap.add_argument("--rows", type=int, default=200_000)
    a = ap.parse_args()
    print(make_sample(a.out, a.rows))
