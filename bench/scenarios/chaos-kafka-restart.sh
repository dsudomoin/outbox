#!/usr/bin/env bash
# chaos kafka-restart: 3 instances at RATE msg/s each (order mode), DURATION s; FAULT_AT s in, `docker
# restart` of the Kafka container under load. Pass: clean (the backlog drains); the instances recover by themselves.
source "$(dirname "$0")/chaos-lib.sh"
chaos_restart kafka-restart bench_chaos_kafka_restart "$KAFKA"
