"""A2 Stage 1: scan-bound filter/project on one machine; also calibrates MB/s per core.
Runs identically on a local sample and on s3:// paths (httpfs) from the EC2 instance."""
import argparse, os, resource, time, duckdb

PROJECTED = ["tpep_pickup_datetime", "PULocationID", "trip_distance",
             "passenger_count", "fare_amount", "tip_amount"]


def run_stage1(trips_glob, out_path, window):
    con = duckdb.connect()
    if trips_glob.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
        # httpfs signs nothing on its own, so on the EC2 instance every s3:// read
        # comes back 403 until a secret exists. The aws extension's credential_chain
        # provider is what reaches the instance profile.
        # The region belongs in the secret itself: a secret with no region makes httpfs
        # guess one, and a separate `SET s3_region` does not reach a credential_chain
        # secret, so the two can disagree and every read 400s.
        con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, "
                    "REGION 'ca-central-1');")

    meta = con.execute(
        "SELECT path_in_schema, total_compressed_size FROM parquet_metadata(?)",
        [trips_glob]).fetchall()
    bytes_on_disk = sum(sz for _, sz in meta)
    bytes_needed = sum(sz for col, sz in meta if col in PROJECTED)

    # ?::TIMESTAMP casts the bound parameter instead of relying on `TIMESTAMP ?`
    # literal-prefix binding, which duckdb's parser does not accept (ParserException).
    # passenger_count is nullable in the real TLC files and is a Phase B feature;
    # numpy's lstsq raises on a NaN, so every fit would fail. Filter it here, once.
    where = ("tpep_pickup_datetime >= ?::TIMESTAMP AND tpep_pickup_datetime < ?::TIMESTAMP "
              "AND fare_amount > 0 AND trip_distance > 0 AND PULocationID IS NOT NULL "
              "AND passenger_count IS NOT NULL")
    cols = ", ".join(PROJECTED)

    rows_in = con.execute(
        "SELECT count(*) FROM read_parquet(?, union_by_name=true)",
        [trips_glob]).fetchone()[0]

    # Pass 1: read + filter, no write. A bare count(*) would let DuckDB prune every
    # column the WHERE clause does not name, so t_read would price 3 columns and the
    # calibration rate would be wrong by the ratio between 3 and 6. Aggregating each
    # projected column forces the reader to fetch all of the bytes the stage projects.
    t0 = time.monotonic()
    row = con.execute(
        f"SELECT count(*), sum(passenger_count), sum(tip_amount), sum(fare_amount), "
        f"sum(trip_distance), max(tpep_pickup_datetime), max(PULocationID) "
        f"FROM (SELECT {cols} FROM read_parquet(?, union_by_name=true) WHERE {where})",
        [trips_glob, *window]).fetchone()
    t_read = time.monotonic() - t0
    rows_out = row[0]

    # COPY ... TO ? genuinely misbinds when combined with other placeholders in the
    # same statement on this duckdb build (values land in the wrong positions —
    # see fix report for the exact reproduced exception), so only the COPY target
    # path is inlined here, with '' escaping; every other path/param above binds.
    out_lit = out_path.replace("'", "''")
    t0 = time.monotonic()                                   # pass 2: same + write
    con.execute(
        f"COPY (SELECT {cols} FROM read_parquet(?, union_by_name=true) WHERE {where}) "
        f"TO '{out_lit}' (FORMAT PARQUET)", [trips_glob, *window])
    t_total = time.monotonic() - t0
    t_write = max(0.0, t_total - t_read)

    cores = os.cpu_count() or 1
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # ru_maxrss is KB on Linux, bytes on macOS.
    peak_rss_mb = rss_kb / (1024 if os.uname().sysname == "Linux" else 1024 * 1024)

    # wall_s is the stage's headline duration (the second pass end to end: read, filter
    # and write), so the parser has the same key here as it has for every Spark phase.
    # ended_at_epoch_s lets Q3.1 bound the gap between one stage and the next.
    return {"wall_s": round(t_total, 1), "t_total_s": round(t_total, 2),
            "bytes_on_disk": bytes_on_disk, "bytes_needed": bytes_needed,
            "rows_in": rows_in, "rows_out": rows_out,
            "t_read_s": round(t_read, 2), "t_write_s": round(t_write, 2),
            "peak_rss_mb": round(peak_rss_mb, 1),
            "mb_per_s_per_core": round(bytes_needed / 1e6 / max(t_read, 1e-9) / cores, 1),
            "ended_at_epoch_s": round(time.time(), 1)}


if __name__ == "__main__":
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
    from stages.stage2_spark import format_block

    ap = argparse.ArgumentParser()
    ap.add_argument("--trips", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--window-start", default="2022-01-01")
    ap.add_argument("--window-end", default="2025-04-01")
    a = ap.parse_args()
    kv = run_stage1(a.trips, a.out, (a.window_start, a.window_end))
    print(format_block({"mode": "duckdb", "platform": "ec2",
                        "stage": "1_duckdb", "phase": "scan"}, kv), flush=True)
