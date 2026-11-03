#!/usr/bin/env bash
# run-pg17.sh [SCENARIO...]: the second section of RESULTS.md, on Postgres 17; results in build/results-pg17, the matrix's stay.
HERE=$(cd "$(dirname "$0")" && pwd)
export PG_VERSION=17
LIST=$(grep -hoE '^s_[a-z0-9_]+\(\)' "$HERE"/s-*.sh | sed -e 's/^s_//' -e 's/()$//' -e 's/_/-/g' |
  grep -E '^(accounts-.*|pg-.*|steady|kill-one|xid-auto|inbox-rotation|retry-long-tail)$')
# shellcheck disable=SC2086
exec "$HERE/run-all.sh" ${*:-$LIST}
