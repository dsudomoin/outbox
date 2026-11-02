# «Соединения: без transaction pooling»: pgbouncer in session and in transaction mode, with and without a direct owner.
POOLER_URL=jdbc:postgresql://localhost:55437/outbox
NO_PREPARE=-Ddb.dsProperties.prepareThreshold=0

pooler_up() { # pooler_up MODE USER SIZE
  AFTER='"${COMPOSE[@]}" --profile pooler rm -fsv pooler'
  POOL_MODE=$1 POOL_USER=$2 POOL_SIZE=$3 "${COMPOSE[@]}" --profile pooler up -d --wait pooler > /dev/null 2>&1
  sleep 1
}

pooling_gauge() { on "$1" metric outbox_maintenance_transaction_pooling; }

s_pgbouncer_session() { # the supported pooler: everything through it, nothing is different
  pc_begin pgbouncer-session
  pooler_up session outbox 120
  PC_PG_URL=$POOLER_URL
  trio "$MIX" -Doutbox.partitions.maintenanceInterval=2s
  sleep 15
  local i
  for i in 1001 1002 1003; do
    check "$(passes is "$(pooling_gauge "$i")" = 0)" "экземпляр $i: outbox.maintenance.transaction_pooling = 0"
  done
  check "$(passes is "$(logs_count 'pg_backend_pid\(\) was')" = 0)" "ни одной строки о transaction pooling"
  check "$(passes is "$(st 1001 'int(d["maintenanceSinceSuccessSeconds"] or 999) < 10')" = True)" "обслуживание проходит: since_success < 10 с"
  pc_kill 1002
  pc_shop 1002 -Doutbox.partitions.maintenanceInterval=2s
  sleep 5
  pc_finish
}

s_pgbouncer_txn() { # the application's pool through transaction pooling, the owner connection direct: what the role section of the README promises
  acc_begin pgbouncer-txn
  acc_full
  psql_q "alter role accounts_rw set search_path = accounts" > /dev/null # a session's search_path means nothing behind this pooler
  local size=8
  pooler_up transaction accounts_rw "$size"
  local direct=$PC_PG_URL
  ACC_OWNER_URL=$direct
  PC_PG_URL=$POOLER_URL
  local opts="$NO_PREPARE -Doutbox.partitions.maintenanceInterval=2s" i
  for i in 1001 1002; do acc "$i" $opts; done
  check 0 "старт проходит: проверка схемы и партиции через прямое соединение владельца"
  for i in 1001 1002; do acc_load "$i"; done
  sleep 12
  pc_kill 1001
  sleep 3
  acc 1001 $opts
  sleep 15
  local servers
  servers=$(psql_q "select count(*) from pg_stat_activity where usename = 'accounts_rw'")
  check "$(passes is "$servers" -le "$size")" "приложение действительно за пулером: серверных соединений $servers при $size в пуле"
  for i in 1001 1002; do
    check "$(passes is "$(pooling_gauge "$i")" = 0)" "экземпляр $i: outbox.maintenance.transaction_pooling = 0: обслуживание идёт через владельца"
  done
  check "$(passes is "$(logs_count 'pg_backend_pid\(\) was')" = 0)" "ни одной строки о transaction pooling"
  check "$(passes is "$(st 1002 'int(d["maintenanceSinceSuccessSeconds"] or 999) < 10')" = True)" "обслуживание проходит: since_success < 10 с"
  check "$(passes is "$(psql_q "select count(*) from accounts.account_events_instance where expires_at > now()")" = 2)" "heartbeat за пулером жив: 2 живые строки"
  acc_end
}

s_pgbouncer_txn_all() { # everything through transaction pooling: maintenance sees it and steps aside, delivery goes on
  pc_begin pgbouncer-txn-all
  pooler_up transaction outbox 6
  PC_PG_URL=$POOLER_URL
  local opts="$NO_PREPARE -Doutbox.partitions.maintenanceInterval=2s"
  # a pool in use: three queries in a row land on different server sessions, bootstrap must refuse
  local name=kora-outbox-prodcheck-pooler log=$LOGS/pc-$SCN-busy-start.log rc=0
  docker exec -d "$name" bash -c 'end=$((SECONDS + 25)); for n in 1 2 3 4 5 6 7 8; do (while [ $SECONDS -lt $end ]; do psql -h 127.0.0.1 -p 6432 -U outbox -d outbox -Atq -c "select pg_sleep(0.02)" > /dev/null 2>&1; done) & done; wait'
  sleep 2
  bounded 20 env HTTP_PORT=9080 METRICS_PORT=9085 POSTGRES_URL="$PC_PG_URL" TARGET_DB_URL="$PC_TARGET_DB_URL" TARGET_URL="$PC_TARGET_URL" INSTANCE_NAME=app-busy "JAVA_OPTS=-Xmx512m $opts" "$SHOP" > "$log" 2>&1 || rc=$?
  log "bootstrap on a busy pool: exit $rc; $(grep -m1 -E 'session|pooling' "$log" | cut -c1-200)"
  check "$(passes grep -qE 'pg_backend_pid|one server session|transaction pooling' "$log")" "старт на занятом пуле отвергнут и называет причину (код $rc): $(grep -m1 -E 'pg_backend_pid|one server session|transaction pooling' "$log" | cut -c1-160)"
  sleep 8
  # a pool of one server session passes the check: it is best effort, the README says; the problem shows in maintenance
  pooler_up transaction outbox 1
  pc_target
  local i
  for i in 1001 1002 1003; do pc_shop "$i" $opts; done
  check 0 "старт на спокойном пуле проходит: проверка best effort, как и сказано в README"
  pooler_up transaction outbox 40 # the size the formula asks for here: relay steps hold a server connection while a handler takes another; dropped once, they come back
  sleep 3
  tfault 'delayMs=5'
  for i in 1001 1002 1003; do pc_load "$i" "$MIX"; done
  sees_it() { [ "$(pooling_gauge 1001)" = 1 ] || [ "$(pooling_gauge 1002)" = 1 ] || [ "$(pooling_gauge 1003)" = 1 ]; }
  check "$(passes wait_for 60 sees_it)" "под нагрузкой gauge outbox.maintenance.transaction_pooling = 1"
  check "$(passes logs_have 'pg_backend_pid\(\) was')" "ERROR обслуживания: $(cat "$LOGS/pc-$SCN-"100*.log | grep -m1 'pg_backend_pid() was' | cut -c1-200)"
  sleep 20
  local lines per
  lines=$(cat "$LOGS/pc-$SCN-"100*.log | grep -c 'pg_backend_pid() was' || true)
  check "$(passes is "$lines" -le $((3 * 3)))" "ERROR не чаще раза в минуту на экземпляр: $lines строк"
  check "$(passes effects_grow 5)" "доставка идёт: send и relay не требуют одной сессии"
  log "advisory locks left on server sessions: $(psql_q "select count(*) from pg_locks where locktype = 'advisory'"), premade $(st 1001 'd["premade"]'), since_success $(st 1001 'd["maintenanceSinceSuccessSeconds"]')"
  log "outbox.inbox gauge: $(on 1001 metric outbox_inbox_maintenance_transaction_pooling) $(on 1002 metric outbox_inbox_maintenance_transaction_pooling)"
  REPAIRED="прямое соединение владельца (pgbouncer-txn) или сессионный режим; потерь нет, пока запаса партиций хватает"
  pc_finish
}
