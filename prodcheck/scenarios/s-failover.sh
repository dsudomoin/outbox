# «Репликация, failover и PITR», «Поддерживаемые версии». A real streaming replica; the apps find the primary themselves.
REPLICA=kora-outbox-prodcheck-replica
MULTI_HOST='jdbc:postgresql://localhost:55432,localhost:55436/outbox?targetServerType=primary&hostRecheckSeconds=2'

replica_up() { # replica_up: lets the replica in, streams from the source; everything goes with the scenario
  AFTER='"${COMPOSE[@]}" --profile failover --profile pooler down -v'
  docker exec "$PG" sh -c 'echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"'
  psql_q "select pg_reload_conf()" > /dev/null
  "${COMPOSE[@]}" --profile failover up -d --wait replica > /dev/null
  streaming() { [ "$(psql_q "select count(*) from pg_stat_replication where state = 'streaming'")" = 1 ]; }
  wait_for 60 streaming
}

kill_primary() {
  docker exec -u postgres "$PG" pg_ctl -D /var/lib/postgresql/data stop -m immediate > /dev/null 2>&1 || true
  fault "pg_ctl stop -m immediate на основной под нагрузкой"
  gone() { [ "$(docker inspect -f '{{.State.Running}}' "$PG")" = false ]; }
  wait_for 20 gone
}

promote() {
  sleep "${1:-3}"
  docker exec -u postgres "$REPLICA" pg_ctl -D /var/lib/postgresql/data promote > /dev/null
  fault "реплика повышена (команда оператора или оркестратора); приложения никто не трогал"
  PG=$REPLICA
  writable() { [ "$(psql_q "select pg_is_in_recovery()")" = f ]; }
  wait_for 30 writable
}

committed_sum() { local i n=0; for i in 1001 1002 1003; do n=$((n + $(stats "$i" | j 'd["committed"]'))); done; echo "$n"; }

s_pg_failover_sync() { # synchronous replica: nothing a client saw committed is lost, the ledger stays strict
  pc_begin pg-failover-sync
  replica_up
  psql_q "alter system set synchronous_standby_names = '*'" > /dev/null
  psql_q "select pg_reload_conf()" > /dev/null
  sync_on() { [ "$(psql_q "select count(*) from pg_stat_replication where sync_state = 'sync'")" = 1 ]; }
  check "$(passes wait_for 30 sync_on)" "реплика синхронная (sync_state = sync)"
  PC_PG_URL=$MULTI_HOST
  trio "$MIX"
  sleep 15
  kill_primary
  promote
  local t=$(now)
  sleep 25
  progress 1001 1002 1003
  log "failover: writers back in under $(($(now) - t)) s"
  check "$(passes is "$(st 1001 'd["halted"]')" = False)" "relay не остановлен: регрессии xid нет"
  check "$(passes is "$(logs_count 'xid regression')" = 0)" "в логах нет ни одного сообщения о регрессии xid"
  REPAIRED="повышение реплики делает оператор; приложения сами нашли новую основную"
  pc_finish
}

s_pg_failover_async() { # asynchronous replica that really lags: the commits of the lost tail are gone on both sides, phantoms at the target are theirs
  pc_begin pg-failover-async
  replica_up
  PC_PG_URL=$MULTI_HOST
  trio "$MIX"
  sleep 12
  # the stream stops for 8 s under load, as a network or a slow standby does; the primary then dies with what it has not sent
  docker exec "$REPLICA" psql -U outbox -d outbox -Atq -c "alter system set primary_conninfo = ''" -c "select pg_reload_conf()" > /dev/null
  fault "реплика перестала получать WAL, основная работает 8 с"
  sleep 8
  local lag primary_lsn
  primary_lsn=$(psql_q "select pg_current_wal_lsn()")
  lag=$(docker exec "$REPLICA" psql -U outbox -d outbox -Atq -c "select pg_wal_lsn_diff('$primary_lsn', pg_last_wal_replay_lsn())")
  kill_primary
  local acked kept
  acked=$(committed_sum)
  kept=$(docker exec "$REPLICA" psql -U outbox -d outbox -Atq -c "select count(*) from pc_sent")
  log "the replica was $lag bytes behind; clients saw $acked commits, the replica has $kept"
  check "$(passes gt "$acked" "$kept")" "отставание настоящее: клиенты видели $acked коммитов, на реплике $kept, потеряно $((acked - kept)), отставание $lag байт"
  promote
  sleep 25
  progress 1001 1002 1003
  check "$(passes is "$(st 1001 'd["halted"]')" = False)" "relay не остановлен"
  local regress
  regress=$(logs_count 'xid regression')
  log "xid regression lines in the logs: $regress"
  check "$(passes is "$regress" = 0)" "счётчик xid на реплике не ниже тиков: путь регрессии xid не сработал ($regress строк)"
  REPAIRED="повышение реплики делает оператор; потерянные коммиты сверяет бизнес: у цели есть эффекты сообщений, которых в базе больше нет"
  pc_finish --allow-phantoms
}

s_pg_version() { # the image and the compose file on another major version; the groups run on it with PG_VERSION=17
  export PG_VERSION=17
  PG_IMAGE=kora-outbox-prodcheck-pg:17
  AFTER='"${COMPOSE[@]}" --profile failover --profile pooler down -v'
  pc_begin pg-version
  local v
  v=$(psql_q "show server_version_num")
  check "$(passes is "${v:0:2}" = 17)" "сервер и источника, и цели на 17: $v, $(target_q 'show server_version_num')"
  local fake
  fake=$(docker run --rm -e LD_PRELOAD="$FAKETIME_LIB/libfaketime.so.1" -e FAKETIME=+1d --entrypoint date "$PG_IMAGE" +%F)
  check "$(passes is "$fake" != "$(date -u +%F)")" "libfaketime на прежнем пути работает в образе 17: date даёт $fake"
  docker exec -i "$PG" psql -U outbox -d postgres -q -v ON_ERROR_STOP=1 -c "create database accounts_check" > /dev/null
  docker exec -i "$PG" psql -U outbox -d accounts_check -q -v ON_ERROR_STOP=1 < "$PC/accounts/roles.sql"
  docker exec -i "$PG" psql -U accounts_owner -d accounts_check -q -v ON_ERROR_STOP=1 < "$PC/accounts/schema.sql"
  check 0 "схема accounts принята на 17"
  trio "$MIX"
  sleep 15
  progress 1001 1002 1003
  pc_finish
}
