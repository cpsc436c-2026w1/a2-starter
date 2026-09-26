#!/bin/bash
# EMR bootstrap action: runs on every node, as the hadoop user, before the cluster is ready.
# Builds a Python environment with uv that Spark can use on the driver and in the executors.
set -euo pipefail

PANDAS=${1:-3.0.6}      # EMR emr-spark-8.0.0 ships pandas 3.0.2; this is an update
DUCKDB=${2:-1.5.5}

# uv itself, and uv's own Pythons, go somewhere every user can read: executors run as `yarn`,
# not as the user who ran this script.
curl -LsSf https://astral.sh/uv/install.sh | sudo env UV_INSTALL_DIR=/usr/local/bin UV_NO_MODIFY_PATH=1 sh
export UV_PYTHON_INSTALL_DIR=/opt/uv-python

make_env() {
  sudo -E /usr/local/bin/uv venv --python 3.11 "$1"
  sudo -E /usr/local/bin/uv pip install --python "$1/bin/python" \
    "pandas==$PANDAS" "duckdb==$DUCKDB" pyarrow numpy
}

make_env /opt/a2-venv                                   # every node: the driver and every executor

# a second copy on the primary node only, to show what a driver-only install does
if grep -q '"isMaster": true' /mnt/var/lib/info/instance.json; then
  make_env /opt/a2-driver-only
fi

sudo chmod -R a+rX /opt/uv-python /opt/a2-venv /opt/a2-driver-only 2>/dev/null || true
