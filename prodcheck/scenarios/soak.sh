#!/usr/bin/env bash
# soak.sh [MINUTES]: the chaos soak, default 60 minutes; the run and its series in build/results/soak.*.
SOAK_MINUTES=${1:-60} exec "$(dirname "$0")/run-all.sh" soak
