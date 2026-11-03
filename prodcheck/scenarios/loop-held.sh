#!/usr/bin/env bash
# loop-held.sh [N]: accounts-same-type-held up to N times (default 20), stops at the first run in which a handler saw a
# redelivered record differ from its first delivery. That run's directory keeps the reasons (.mismatch), the first-seen
# values (.seen), the arrivals of those messages (.arrivals) and the instances' logs. Exit 0 clean, 1 a mismatch, 2 a
# host that slept: such a run says nothing.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
S=accounts-same-type-held
ROOT_DIR=$HERE/../build/results-held-$(date +%Y%m%d-%H%M%S)
for n in $(seq 1 "${1:-20}"); do
  export RESULT_DIR=$ROOT_DIR/$n
  mkdir -p "$RESULT_DIR"
  echo "run $n: $("$HERE/run-all.sh" "$S" | tail -1)"
  if grep -q host_slept "$RESULT_DIR/$S.json"; then
    echo "run $n: the host slept: stopping, see $RESULT_DIR"
    exit 2
  fi
  if [ -s "$RESULT_DIR/$S.mismatch" ]; then
    echo "run $n: mismatch, see $RESULT_DIR"
    cut -c1-400 "$RESULT_DIR/$S.mismatch"
    exit 1
  fi
done
echo "no mismatch in ${1:-20} runs: $ROOT_DIR"
