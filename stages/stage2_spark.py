"""A2 Stage 2/3 job. One file, runs unchanged under EMR spark-submit and as a Glue 6.0
job script. Metrics via the driver's own REST API (see plan D1) — no SparkListener,
no History Server, no platform-specific code paths."""
import argparse, json, sys, time, urllib.parse, urllib.request

LISTENER_VERSION = "2.0"

PROBE_TIMEOUT_S = 5      # a dead candidate host must not stall the run

def _get(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)

def format_block(header, kv):
    head = " ".join(f"{k}={v}" for k, v in header.items())
    body = "\n".join(f"{k}={v}" for k, v in kv.items())
    return f"===A2-METRICS v{LISTENER_VERSION} {head}===\n{body}\n===END==="

class MetricsCollector:
    def __init__(self, spark, platform, stage, allow_degraded=False):
        self.spark, self.platform, self.stage = spark, platform, stage
        # read back rather than passed in, so the number in the block is the number the
        # session is actually running with. Phase A's whole scaling story is chunks
        # against cores, so a block that did not carry it would be unreadable.
        self.shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
        sc = spark.sparkContext
        self.base = None
        self.rest_target = None      # "advertised" | "localhost", reported in every block
        tried = []
        if sc.uiWebUrl:
            # The collector runs in the same process as the driver whose UI it queries,
            # and which host answers is platform-dependent — in opposite directions:
            #   * local / Glue: the advertised host is a LAN IP or container address that
            #     is not reachable from the driver's own process (observed: it resets the
            #     connection), while the same port on localhost answers fine;
            #   * EMR client mode: the driver UI sits behind YARN's AmIpFilter, which
            #     matches on the host the request is addressed to. localhost is 302'd to
            #     the YARN proxy, which answers 200 with an HTML page — so the dial
            #     "succeeds" and json.load then blows up. Only the advertised host from
            #     sc.uiWebUrl returns the API's JSON (2026-08-31 pilot, Deviation 8).
            # So probe both, advertised first, and accept a candidate only when the body
            # parses as a JSON *list*: an HTML 200 (or any other shape a future proxy
            # might answer with) must count as a failure, not as a working endpoint that
            # then reports zeros. Scheme is preserved because a cluster may run
            # spark.ui.https.enabled=true; the port is preserved or guessed because a
            # bare host with no explicit port would otherwise produce "host:None".
            parsed = urllib.parse.urlsplit(sc.uiWebUrl)
            port = parsed.port or 4040
            adv = parsed.hostname
            hosts = [("localhost", "localhost")]
            if adv and adv not in ("localhost", "127.0.0.1", "::1"):
                hosts.insert(0, ("advertised", adv))     # advertised first
            for target, host in hosts:
                candidate = (f"{parsed.scheme}://{host}:{port}"
                             f"/api/v1/applications/{sc.applicationId}")
                tried.append(candidate)
                try:
                    probe = _get(f"{candidate}/stages?status=complete",
                                 timeout=PROBE_TIMEOUT_S)
                except Exception:
                    continue
                if isinstance(probe, list):
                    self.base, self.rest_target = candidate, target
                    break
        self.mode = "rest" if self.base else "degraded"
        if self.mode == "degraded" and not allow_degraded:
            print(f"FATAL: Spark UI REST endpoint unreachable (uiWebUrl="
                  f"{sc.uiWebUrl!r}). Tried, in order: "
                  f"{', '.join(tried) if tried else '(no candidates: no uiWebUrl)'}. "
                  f"Each must answer a JSON list at /stages?status=complete; an HTML "
                  f"reply (an EMR/YARN proxy page, for instance) counts as unreachable. "
                  f"Metrics would be incomplete. Re-run with spark.ui.enabled=true, or "
                  f"pass --allow-degraded.", file=sys.stderr)
            raise SystemExit(3)
        self.results = {}

    def _completed_ids(self):
        return {s["stageId"] for s in _get(f"{self.base}/stages?status=complete")}

    def phase(self, name):
        return _Phase(self, name)

    def format_phase(self, name):
        r = self.results[name]
        kv = {k: r[k] for k in ("wall_s", "shuffle_write_bytes", "partitions", "tasks",
                                "retried", "task_min_s", "task_med_s", "task_max_s",
                                "ended_at_epoch_s")}
        # which host the REST dial landed on, so a log tells you whether the run went
        # through the advertised host (EMR) or localhost (Glue/local) without guessing
        kv["rest_target"] = self.rest_target or "none"
        kv["shuffle_partitions"] = self.shuffle_partitions
        kv.update(r.get("extra", {}))
        return format_block(
            {"mode": self.mode, "platform": self.platform,
             "stage": self.stage, "phase": name}, kv)

    def print_block(self, name):
        print(self.format_phase(name), flush=True)

def _aggregate_stages(stages, quantile_fetch):
    """Pure aggregation over one phase's raw `/stages?status=complete` entries (already
    filtered to stageIds that completed inside the phase — see `before` in `_Phase`).

    A Spark stage that is resubmitted after an executor failure (OOM, fetch failure,
    ...) appears more than once in that list, once per attemptId, all reporting
    `status=complete` once they finish. Summing across attempts double- (or triple-)
    counts shuffle bytes and tasks, and `max(numTasks)` can pick a partially-coalesced
    retry's task count instead of the original stage's — see
    a2-window-pilot-report.md, "attempt" (2026-08-31 Glue pilot: 9.88 GB reported vs
    4.18 GB true, partitions=14 instead of 64). So: group by stageId, keep only the
    highest attemptId per stageId, and aggregate over that deduped set only. Never sum
    across attempts.

    `quantile_fetch(stage_id, attempt_id) -> (min_ms, med_ms, max_ms)` fetches the
    executorRunTime quantiles for one stage attempt (raises on failure, same as the
    REST call it wraps) — injected so this function needs no live Spark UI to test.

    `partitions` is the post-shuffle chunk count, so it is read off the stage(s) that
    *consume* the shuffle (`shuffleReadBytes > 0`), latest attempt only, and not off the
    widest stage in the phase. Phase A's scan splits by input bytes and by the cluster's
    core count, so its task count moves with the node count (10 at 2 core nodes and 16 at
    4 on pilot 4). A max over every stage therefore reported the scan's
    number rather than the pin, and drifted with the cluster, which hides the very
    constant Stage 2 is built on. A phase that read no shuffle at all (phase B) falls back
    to the widest-stage max.

    `tasks` stays the sum over all tasks in the phase, scan included, so it grows with
    cores. That is what it is for: the phase's total task count, not its chunk count.

    A stage attempt can also retry *tasks* without ever being resubmitted itself: an
    executor lost to a heartbeat timeout takes its running tasks down with it, and Spark
    re-runs those tasks inside the same stage attempt (no new attemptId, so the
    stage-attempt check above never fires). The REST stage JSON carries this as
    `numFailedTasks` (missing key treated as 0) on the attempt. A 2026-09-01 EMR pilot
    lost an executor mid-phase this way: the phase took 686 s instead of 118 s and the
    block still read `retried=0` (a2-confirm-pilot-report.md, "DEVIATION F"). So: also
    count a stageId as retried when its *latest* attempt reports `numFailedTasks > 0`.

    Returns (out, retried_stage_ids, failed_task_counts) where `out` carries the usual kv
    keys plus `retried` (1 if any stageId here had more than one attempt, any attempt did
    not report status COMPLETE, or the latest attempt had numFailedTasks > 0; else 0),
    `retried_stage_ids` is the sorted list of stageIds responsible, for the caller to name
    in a stderr warning, and `failed_task_counts` maps stageId -> numFailedTasks for the
    stageIds among those whose latest attempt had numFailedTasks > 0.
    """
    attempts_by_id = {}
    for s in stages:
        attempts_by_id.setdefault(s["stageId"], []).append(s)

    retried_ids = []
    failed_task_counts = {}
    latest_by_id = {}
    for sid, attempts in attempts_by_id.items():
        latest = max(attempts, key=lambda a: a.get("attemptId", 0))
        latest_by_id[sid] = latest
        n_failed = latest.get("numFailedTasks", 0)
        if (len(attempts) > 1
                or any(a.get("status", "COMPLETE") != "COMPLETE" for a in attempts)
                or n_failed > 0):
            retried_ids.append(sid)
        if n_failed > 0:
            failed_task_counts[sid] = n_failed
    retried_ids.sort()

    out = {"shuffle_write_bytes": 0, "partitions": 0, "tasks": 0, "retried": 0,
           "task_min_s": 0.0, "task_med_s": 0.0, "task_max_s": 0.0}
    durs_min, durs_med, durs_max = [], [], []
    widest = 0                  # fallback for a phase that reads no shuffle
    reduce_side = None          # numTasks of the stage(s) that read the shuffle
    for s in latest_by_id.values():
        out["shuffle_write_bytes"] += s.get("shuffleWriteBytes", 0)
        out["tasks"] += s.get("numCompleteTasks", 0)
        n_tasks = s.get("numTasks", 0)
        widest = max(widest, n_tasks)
        if s.get("shuffleReadBytes", 0) > 0:
            reduce_side = n_tasks if reduce_side is None else max(reduce_side, n_tasks)
        try:
            lo, med, hi = quantile_fetch(s["stageId"], s.get("attemptId", 0))
            durs_min.append(lo); durs_med.append(med); durs_max.append(hi)
        except Exception:
            pass
    out["partitions"] = widest if reduce_side is None else reduce_side
    if durs_max:
        out["task_min_s"] = round(min(durs_min) / 1000, 1)
        out["task_med_s"] = round(max(durs_med) / 1000, 1)   # slowest stage's median
        out["task_max_s"] = round(max(durs_max) / 1000, 1)
    if retried_ids:
        out["retried"] = 1
    return out, retried_ids, failed_task_counts

class _Phase:
    def __init__(self, mc, name):
        self.mc, self.name = mc, name
        self.extra = {}
    def __enter__(self):
        self.t0 = time.monotonic()
        self.before = self.mc._completed_ids() if self.mc.mode == "rest" else set()
        return self
    def _sweep(self):
        """One REST pass over the stages that completed inside this phase. Builds a fresh
        dict so a retry after a partial read cannot double-count, and dedupes retried
        stage attempts down to the latest attempt only (see `_aggregate_stages`)."""
        stages = [s for s in _get(f"{self.mc.base}/stages?status=complete")
                  if s["stageId"] not in self.before]

        def quantile_fetch(stage_id, attempt_id):
            q = _get(f"{self.mc.base}/stages/{stage_id}/{attempt_id}/"
                     f"taskSummary?quantiles=0.0,0.5,1.0")
            return q["executorRunTime"]

        out, retried_ids, failed_task_counts = _aggregate_stages(stages, quantile_fetch)
        if retried_ids:
            detail = (f" failed task counts: {failed_task_counts};" if failed_task_counts
                       else "")
            print(f"WARNING: phase {self.name} stage(s) {retried_ids} were retried "
                  f"(multiple attempts completed, an attempt failed, or task(s) failed "
                  f"within a single attempt);{detail} aggregating the latest attempt of "
                  f"each only. Compare this block's numbers with caution.",
                  file=sys.stderr, flush=True)
        return out

    def __exit__(self, *exc):
        wall = time.monotonic() - self.t0
        # ended_at_epoch_s is wall-clock, not monotonic: Q4.1 subtracts one block's stamp
        # from another's to bound the gap between two phases.
        r = {"wall_s": round(wall, 1), "shuffle_write_bytes": 0, "partitions": 0,
             "tasks": 0, "retried": 0, "task_min_s": 0.0, "task_med_s": 0.0,
             "task_max_s": 0.0, "ended_at_epoch_s": round(time.time(), 1),
             "extra": self.extra}
        if self.mc.mode == "rest" and not exc[0]:
            # The work is already done by the time we read the REST endpoint. A blip
            # there must not take the run down with it: retry, then report zeros and
            # say so in the block rather than raising out of a finished phase.
            for attempt in range(3):
                try:
                    r.update(self._sweep())
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"WARNING: metrics REST read failed 3 times for phase "
                              f"{self.name} ({e!r}); reporting zeros, mode=degraded.",
                              file=sys.stderr, flush=True)
                        self.extra["mode"] = "degraded"
                    else:
                        time.sleep(0.5)
        self.mc.results[self.name] = r
        return False

# --- Stage 2 phases (part 2 of 3) ------------------------------------------
# Phase A = broadcast join + a groupBy on PULocationID run through applyInPandas, which
# spreads the zones over spark.sql.shuffle.partitions chunks, pinned to 16 (decision 8).
# The reveal is chunks against cores: 16 chunks on 8 cores is two rounds and on 16 cores
# is one, so extra nodes help phase A less than they help phase B, and the pin must not be
# optimized away. 16 is also the memory-safe setting — at 8 the rows per chunk double and
# a 16 GB worker loses executors. applyInPandas still earns its place because it gets no
# partial aggregation, which keeps the shuffle honest, but skew is not the lesson here:
# the top zone is under 5% of the rows. Phase B = task-parallel numpy-only model fits over
# a broadcast sample.
import numpy as np, pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType

FEAT_SCHEMA = StructType([
    StructField("PULocationID", IntegerType()),
    StructField("n_trips", LongType()),
    StructField("mean_fare", DoubleType()),
    StructField("mean_tip_ratio", DoubleType()),
    StructField("p90_distance", DoubleType()),
    StructField("fare_per_km", DoubleType()),
])

def _zone_features(pdf: pd.DataFrame) -> pd.DataFrame:
    # deliberately does real per-row work so tasks have weight. applyInPandas gets no
    # partial aggregation (plan D3), so every row of a zone really crosses the shuffle to
    # the one chunk that zone hashes to. That keeps the shuffle honest; what caps phase A
    # is the chunk count, not a hot key.
    tip_ratio = (pdf["tip_amount"] / pdf["fare_amount"].clip(lower=0.01)).to_numpy()
    return pd.DataFrame([{
        "PULocationID": int(pdf["PULocationID"].iloc[0]),
        "n_trips": len(pdf),
        "mean_fare": float(pdf["fare_amount"].mean()),
        "mean_tip_ratio": float(np.mean(tip_ratio)),
        "p90_distance": float(np.percentile(pdf["trip_distance"], 90)),
        "fare_per_km": float((pdf["fare_amount"] / pdf["trip_distance"].clip(lower=0.1)).median()),
    }])

def run_phase_a(spark, mc, trips_path, zones_path, out_path):
    trips = spark.read.parquet(trips_path)
    zones = spark.read.parquet(zones_path)
    with mc.phase("A_aggregate"):
        joined = trips.join(F.broadcast(zones),
                            trips.PULocationID == zones.LocationID, "left")
        feat = joined.groupby("PULocationID").applyInPandas(
            _zone_features, schema=FEAT_SCHEMA)
        feat.write.mode("overwrite").parquet(out_path)
    mc.print_block("A_aggregate")
    return spark.read.parquet(out_path)

def fit_one(args):
    """One (combo, fold) fit. This is a compute kernel — scores are not meaningful,
    the point is that each fit is an independent lump of arithmetic."""
    i, fold, Xy, fit_rounds, folds = args
    X, y = Xy
    idx = np.arange(len(y)); mask = (idx % folds) != fold
    Xt, yt = X[mask], y[mask].astype(float)
    pred = np.zeros_like(yt); resid = yt.copy()
    for _ in range(fit_rounds):                     # boosted ridge, numpy-only (plan D5)
        w, *_ = np.linalg.lstsq(Xt, resid, rcond=None)
        step = Xt @ w
        pred += 0.5 * step; resid = yt - pred
    Xv, yv = X[~mask], y[~mask].astype(float)
    return float(np.mean((Xv @ w * 0.5 + np.mean(pred) - yv) ** 2))

def run_phase_b(spark, mc, trips_path, feat_df, combos, folds, fit_rounds,
                sample_rows=200_000):
    sc = spark.sparkContext
    trips = spark.read.parquet(trips_path).select(
        "PULocationID", "trip_distance", "passenger_count", "fare_amount")
    frac = min(1.0, sample_rows / max(trips.count(), 1))
    pdf = trips.sample(fraction=frac, seed=436).join(
        feat_df, on="PULocationID", how="inner").toPandas()
    feats = ["trip_distance", "passenger_count", "mean_fare",
             "mean_tip_ratio", "p90_distance", "fare_per_km"]
    # A single NaN anywhere in X makes np.linalg.lstsq raise, which would fail every
    # task in the grid. Stage 1 already filters the nullable input column; this is the
    # second half of the belt, because the join can widen a row with nulls too.
    pdf = pdf.dropna(subset=feats + ["fare_amount"])
    X = np.column_stack([np.ones(len(pdf))] + [pdf[f].to_numpy(float) for f in feats])
    y = pdf["fare_amount"].to_numpy(float)
    bc = sc.broadcast((X, y))
    grid = [(c, f) for c in range(combos) for f in range(folds)]
    with mc.phase("B_fit") as ph:
        scores = (sc.parallelize(grid, len(grid))
                    .map(lambda cf: fit_one((cf[0], cf[1], bc.value,
                                             fit_rounds * (1 + cf[0]), folds)))
                    .collect())
        cores = int(sc.defaultParallelism)
        ph.extra["fits_done"] = len(scores)
        ph.extra["effective_parallelism"] = min(cores, len(grid))
    mc.print_block("B_fit")
    return scores

# --- Entry point (part 3 of 3) ----------------------------------------------
# Portable across EMR spark-submit and AWS Glue job scripts: parse_known_args
# tolerates Glue-injected args (e.g. --JOB_NAME) instead of erroring on them.
def main(argv=None):
    # allow_abbrev=False: Glue injects arguments of its own, and with abbreviation on,
    # an injected flag that happens to be a prefix of one of ours (--co, --pl) would be
    # swallowed as --combos or --platform instead of landing in the ignored remainder.
    ap = argparse.ArgumentParser(allow_abbrev=False)
    ap.add_argument("--trips", required=True)
    ap.add_argument("--zones", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--platform", choices=["emr", "glue", "local"], required=True)
    ap.add_argument("--combos", type=int, default=12)
    ap.add_argument("--folds", type=int, default=5)
    ap.add_argument("--fit-rounds", type=int, default=50)
    # 16 = twice the 2-core-node cluster's executor cores (decision 8). Phase A runs the
    # same 16 chunks at either node count: two rounds on 8 cores, one on 16, which is the
    # point of Stage 2. 16 also keeps rows per chunk under the memory cliff that cost
    # executors at 8. Students re-run at 8 in the optional extension to see the ceiling.
    ap.add_argument("--shuffle-partitions", type=int, default=16)
    ap.add_argument("--allow-degraded", action="store_true")
    args, _unknown = ap.parse_known_args(argv)     # Glue injects --JOB_NAME etc. (plan D2)

    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName("a2-stage2")
    if args.platform == "local":
        builder = builder.master("local[*]")
    # Pinning shuffle.partitions is not enough on its own: adaptive execution
    # coalesces the post-shuffle partitions back down (observed: 64 pinned, 2 run), so
    # the partition count would be a property of the data volume and of each platform's
    # AQE defaults instead of a constant of the job. Stage 3 compares that number
    # across platforms, so it has to be fixed at both ends.
    spark = (builder.config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                    .config("spark.ui.enabled", "true").getOrCreate())
    stage = {"emr": "2_emr", "glue": "3_glue", "local": "2_emr"}[args.platform]
    mc = MetricsCollector(spark, platform=args.platform, stage=stage,
                          allow_degraded=args.allow_degraded)
    feat = run_phase_a(spark, mc, args.trips, args.zones, args.out + "/features")
    run_phase_b(spark, mc, args.trips, feat, args.combos, args.folds, args.fit_rounds)
    spark.stop()

if __name__ == "__main__":
    main()
