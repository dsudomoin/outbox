#!/usr/bin/env bash
# run.sh SCENARIO: one scenario of the matrix (the functions s_* in the s-*.sh files). See ../README.md.
source "$(dirname "$0")/lib.sh"
for f in "$PC"/scenarios/s-*.sh; do source "$f"; done
"s_${1//-/_}"
