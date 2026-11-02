#!/usr/bin/env bash
# run-all.sh [SCENARIO...]: the whole matrix, or the given scenarios, each in its own process; goes on after a failure
# and prints one line per scenario. The stand lock is taken once, for the whole run.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/../../bench/scenarios/stand-lock.sh"
stand_acquire
trap stand_release EXIT
# A sleeping Mac freezes the JVMs and the Docker VM while deadlines run: such a run says nothing, so it is marked.
if command -v caffeinate > /dev/null; then caffeinate -s -w $$ & fi
slept_since() { # slept_since "YYYY-MM-DD HH:MM:SS": system sleeps the power log has since then
  command -v pmset > /dev/null || { echo 0; return; }
  pmset -g log | awk -v from="$1" '/Entering Sleep state/ && ($1 " " $2) >= from { n++ } END { print n + 0 }'
}
RESULT_DIR=${RESULT_DIR:-$HERE/../build/results}
mkdir -p "$RESULT_DIR"
ALL=$(grep -hoE '^s_[a-z0-9_]+\(\)' "$HERE"/s-*.sh | sed -e 's/^s_//' -e 's/()$//' -e 's/_/-/g')
for s in ${*:-$ALL}; do
  rm -f "${RESULT_DIR:?}/${s:?}.json"
  from=$(date '+%Y-%m-%d %H:%M:%S')
  bash "$HERE/run.sh" "$s" > /dev/null 2>> "$RESULT_DIR/$s.log"
  rc=$?
  [ -f "$RESULT_DIR/$s.json" ] || echo "{\"scenario\": \"$s\", \"verdict\": \"ABORTED (rc $rc)\", \"repaired\": \"\", \"ledger\": {}, \"checks\": []}" > "$RESULT_DIR/$s.json"
  slept=$(slept_since "$from")
  if [ "$slept" != 0 ]; then
    python3 -c 'import json, sys; d = json.load(open(sys.argv[1])); d["host_slept"] = int(sys.argv[2]); json.dump(d, open(sys.argv[1], "w"), ensure_ascii=False, indent=1)' "$RESULT_DIR/$s.json" "$slept"
    echo "$s: THE HOST SLEPT $slept times during this run: its result says nothing"
  fi
  python3 -c 'import json, sys; d = json.load(open(sys.argv[1])); print(d["scenario"], d["verdict"], d["ledger"].get("violations", ""), [c for c in d["checks"] if "НЕ ПРОЙДЕНА" in c])' "$RESULT_DIR/$s.json"
done
