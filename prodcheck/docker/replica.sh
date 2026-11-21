#!/bin/bash
# A hot standby of the compose service `source`: base backup, then the server with the compose command, the primary's
# settings (a standby needs at least its max_connections and max_locks_per_transaction).
set -eu
data=/var/lib/postgresql/data
chown postgres "$data"
chmod 700 "$data"
until gosu postgres pg_basebackup -h source -U outbox -D "$data" -R -X stream -c fast; do
  rm -rf "${data:?}"/*
  sleep 1
done
exec gosu postgres "$@"
