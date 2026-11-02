#!/usr/bin/env bash
# run-all.sh [SCENARIO...]: the whole matrix, or the given scenarios, each in its own process; goes on after a failure
# and prints one line per scenario. The stand lock is taken once, for the whole run.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/../../bench/scenarios/stand-lock.sh"
stand_acquire
trap stand_release EXIT
RESULT_DIR=${RESULT_DIR:-$HERE/../build/results}
mkdir -p "$RESULT_DIR"
ALL=$(grep -hoE '^s_[a-z0-9_]+\(\)' "$HERE"/s-*.sh | sed -e 's/^s_//' -e 's/()$//' -e 's/_/-/g')
for s in ${*:-$ALL}; do
  rm -f "${RESULT_DIR:?}/${s:?}.json"
  bash "$HERE/run.sh" "$s" > /dev/null 2>> "$RESULT_DIR/$s.log"
  rc=$?
  [ -f "$RESULT_DIR/$s.json" ] || echo "{\"scenario\": \"$s\", \"verdict\": \"ABORTED (rc $rc)\", \"repaired\": \"\", \"ledger\": {}, \"checks\": []}" > "$RESULT_DIR/$s.json"
  python3 -c 'import json, sys; d = json.load(open(sys.argv[1])); print(d["scenario"], d["verdict"], d["ledger"].get("violations", ""), [c for c in d["checks"] if "НЕ ПРОЙДЕНА" in c])' "$RESULT_DIR/$s.json"
done
