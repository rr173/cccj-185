#!/usr/bin/env bash
# Reproduce every offline-replication result into one workdir.
#
#   functional drill (real pipeline + mock service, all verdicts, fork,
#   concurrency, SIGKILL, source/enrichment removal) -> drill/
#   70k-revision, ~4 KiB-note package under a 256 MiB cap            -> scale/
#   full unittest suite                                              -> test-suite.txt
#
# Generated data and large packages stay inside the workdir and are never
# committed.  All Python runs use isolated mode (-I).
set -euo pipefail

WORKDIR="${1:?usage: run_replication_demo.sh WORKDIR}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$WORKDIR"
PY="$(command -v python3)"

echo "== functional drill =="
"$PY" -I "$ROOT/demo/warehouse/replica_drill.py" \
    --workdir "$WORKDIR/drill" | tee "$WORKDIR/drill-output.txt"

echo "== scale: build (outside memory cap) =="
"$PY" -I "$ROOT/demo/warehouse/replica_scale_driver.py" build \
    --workdir "$WORKDIR/scale" | tee "$WORKDIR/scale-build.txt"

echo "== scale: export under 256 MiB RLIMIT_AS =="
"$PY" -I "$ROOT/demo/warehouse/replica_scale_driver.py" export \
    --workdir "$WORKDIR/scale" | tee "$WORKDIR/scale-export.txt"

echo "== scale: accept under 256 MiB RLIMIT_AS =="
"$PY" -I "$ROOT/demo/warehouse/replica_scale_driver.py" apply \
    --workdir "$WORKDIR/scale" | tee "$WORKDIR/scale-apply.txt"

echo "== scale: verify anchors and prefix equality under cap =="
"$PY" -I "$ROOT/demo/warehouse/replica_scale_driver.py" verify \
    --workdir "$WORKDIR/scale" | tee "$WORKDIR/scale-verify.txt"

echo "== full test suite =="
"$PY" -I "$ROOT/tests/run_all.py" | tee "$WORKDIR/test-suite.txt"

echo "ALL REPLICATION DEMO STEPS COMPLETED in $WORKDIR"
