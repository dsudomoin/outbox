#!/bin/bash
# pgbouncer: every client is served as POOL_USER, whose password is its name in this stand.
set -eu
cat > /tmp/pgbouncer.ini <<INI
[databases]
outbox = host=host.docker.internal port=55432 dbname=outbox user=$POOL_USER password=$POOL_USER

[pgbouncer]
listen_addr = 0.0.0.0
listen_port = 6432
unix_socket_dir =
auth_type = any
pool_mode = $POOL_MODE
default_pool_size = $POOL_SIZE
max_client_conn = 1000
ignore_startup_parameters = extra_float_digits,options,search_path
logfile =
pidfile =
INI
chmod 644 /tmp/pgbouncer.ini
exec gosu postgres pgbouncer /tmp/pgbouncer.ini
