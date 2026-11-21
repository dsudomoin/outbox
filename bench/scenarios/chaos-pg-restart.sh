#!/usr/bin/env bash
# chaos pg-restart: 3 instances at RATE msg/s each (order mode), DURATION s; FAULT_AT s in, `docker
# restart` of the Postgres container under load. Pass: clean; the instances recover by themselves (the same processes
# commit, relay and consume again).
source "$(dirname "$0")/chaos-lib.sh"
chaos_restart pg-restart bench_chaos_pg_restart "$PG"
