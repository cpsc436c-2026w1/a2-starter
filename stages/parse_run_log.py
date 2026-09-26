"""Turn captured run output into results.csv rows — no hand transcription (brief §5)."""
import argparse, re

RESULTS_HEADER = ["stage", "resource_id", "nodes", "phase", "predicted_s", "wall_s",
                  "startup_s", "shuffle_write_bytes", "task_min_s", "task_med_s",
                  "task_max_s", "cost_usd"]
_BLOCK = re.compile(r"===A2-METRICS v(?P<v>[\d.]+) (?P<head>.*?)===\n"
                    r"(?P<body>.*?)\n===END===", re.S)

def parse_blocks(text):
    out = []
    for m in _BLOCK.finditer(text):
        d = {"listener_version": m["v"]}
        d.update(dict(p.split("=", 1) for p in m["head"].split()))
        # a wrapped or interleaved log line inside the block is skipped, not fatal
        d.update(dict(l.split("=", 1) for l in m["body"].strip().splitlines()
                      if "=" in l))
        out.append(d)
    return out

def to_csv_rows(blocks, resource_id, nodes, startup_s=""):
    rows = []
    for b in blocks:
        vals = {"stage": b.get("stage", ""), "resource_id": resource_id,
                "nodes": str(nodes), "phase": b.get("phase", ""),
                "predicted_s": "", "wall_s": b.get("wall_s", ""),
                "startup_s": str(startup_s),
                "shuffle_write_bytes": b.get("shuffle_write_bytes", ""),
                "task_min_s": b.get("task_min_s", ""),
                "task_med_s": b.get("task_med_s", ""),
                "task_max_s": b.get("task_max_s", ""), "cost_usd": ""}
        rows.append(",".join(vals[c] for c in RESULTS_HEADER))
    return rows

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("logfile"); ap.add_argument("--resource-id", required=True)
    ap.add_argument("--nodes", required=True); ap.add_argument("--startup-s", default="")
    a = ap.parse_args()
    text = open(a.logfile).read()
    for r in to_csv_rows(parse_blocks(text), a.resource_id, a.nodes, a.startup_s):
        print(r)
