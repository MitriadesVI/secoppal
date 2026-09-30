#!/bin/bash
# Reproducible full-suite run for the Foundation promotion gates (ADR-102 §15.6).
# usage: foundation_promotion_suite.sh <clean detached worktree> <outdir> <python>
# Network is blocked by scripts/audits/pytest_netblock.py; every attempt is logged per node id.
WT=$1; OUT=$2; PY=${3:-python}
mkdir -p "$OUT"; rm -f "$OUT/netblock.jsonl"
PLUGIN_DIR=$(mktemp -d); cp "$WT/scripts/audits/pytest_netblock.py" "$PLUGIN_DIR/netblock.py"
cd "$WT" || exit 2
START=$(date +%s)
env -i HOME="$HOME" PATH=/usr/bin:/bin:/usr/sbin:/sbin LANG=en_US.UTF-8 TZ=America/Bogota PYTHONHASHSEED=0 \
  PYTHONPATH="$PLUGIN_DIR" NETBLOCK_LOG="$OUT/netblock.jsonl" PYTHONDONTWRITEBYTECODE=1 \
  "$PY" -m pytest tests/ -q -p netblock -p no:cacheprovider --junitxml="$OUT/junit.xml" -rA > "$OUT/pytest.log" 2>&1
echo "exit=$? seconds=$(( $(date +%s) - START ))" > "$OUT/status.txt"
