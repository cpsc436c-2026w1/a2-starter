-- Stage 1, the work only: what DuckDB runs on the one EC2 instance.
-- For reading. The graded run is stages/stage1_duckdb.py, which runs this same query and adds the timers.
--
-- Input:  39 monthly Parquet files, s3://<your bucket>/a2/raw/yellow_tripdata_*.parquet
--         19 columns each (20 in the 2025 files)
-- Output: one Parquet file, s3://<your bucket>/a2/matrix.parquet, 6 columns

-- the files differ slightly between years, so union_by_name lines the columns up by name
COPY (
    SELECT tpep_pickup_datetime,
           PULocationID,
           trip_distance,
           passenger_count,
           fare_amount,
           tip_amount                              -- 6 of the 19 columns
    FROM read_parquet('s3://<your bucket>/a2/raw/yellow_tripdata_*.parquet', union_by_name = true)
    WHERE tpep_pickup_datetime >= TIMESTAMP '2022-01-01'
      AND tpep_pickup_datetime <  TIMESTAMP '2025-04-01'   -- the study window
      AND fare_amount > 0
      AND trip_distance > 0
      AND PULocationID IS NOT NULL
      AND passenger_count IS NOT NULL
) TO 's3://<your bucket>/a2/matrix.parquet' (FORMAT PARQUET);

-- Before this, the script reads the same 6 columns once with the same WHERE clause and writes
-- nothing: that read-only pass is what t_read_s times. wall_s times the COPY above.
