#!/bin/bash
# A hot standby of the compose service `source`: base backup, then the server with the primary's settings.
set -eu
data=/var/lib/postgresql/data
chown postgres "$data"
chmod 700 "$data"
until gosu postgres pg_basebackup -h source -U outbox -D "$data" -R -X stream -c fast; do
  rm -rf "${data:?}"/*
  sleep 1
done
exec gosu postgres postgres -c max_connections=300 -c shared_buffers=512MB -c max_locks_per_transaction=256 -c synchronous_commit=on
