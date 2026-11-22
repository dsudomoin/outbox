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
  trio "$MIX" -Doutbox.maintenanceInterval=2s
  sleep 15
  local i
  for i in 1001 1002 1003; do
    check "$(passes is "$(pooling_gauge "$i")" = 0)" "экземпляр $i: outbox.maintenance.transaction_pooling = 0"
  done
  check "$(passes is "$(logs_count 'pg_backend_pid\(\) was')" = 0)" "ни одной строки о transaction pooling"
  check "$(passes is "$(st 1001 'int(d["maintenanceSinceSuccessSeconds"] or 999) < 10')" = True)" "обслуживание проходит: since_success < 10 с"
  pc_kill 1002
  pc_shop 1002 -Doutbox.maintenanceInterval=2s
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
  local opts="$NO_PREPARE -Doutbox.maintenanceInterval=2s" i
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

s_pgbouncer_txn_all() { # everything through transaction pooling, no direct connection anywhere: start, maintenance and delivery all go on
  pc_begin pgbouncer-txn-all
  pooler_up transaction outbox 40 # the size the formula asks for here: relay steps hold a server connection while a handler takes another
  PC_PG_URL=$POOLER_URL
  local opts="$NO_PREPARE -Doutbox.maintenanceInterval=2s"
  # a pool in use: three queries in a row land on different server sessions, which the start no longer minds
  local name=kora-outbox-prodcheck-pooler
  docker exec -d "$name" bash -c 'end=$((SECONDS + 25)); for n in $(seq 24); do (while [ $SECONDS -lt $end ]; do psql -h 127.0.0.1 -p 6432 -U outbox -d outbox -Atq -c "select pg_sleep(0.02)" > /dev/null 2>&1; done) & done; wait'
  sleep 2
  pc_target
  local i
  for i in 1001 1002 1003; do pc_shop "$i" $opts; done
  check 0 "старт на занятом пуле проходит"
  tfault 'delayMs=5'
  for i in 1001 1002 1003; do pc_load "$i" "$MIX"; done
  sleep 25
  for i in 1001 1002 1003; do
    check "$(passes is "$(pooling_gauge "$i")" = 0)" "экземпляр $i: outbox.maintenance.transaction_pooling = 0"
  done
  check "$(passes is "$(logs_count 'pg_backend_pid\(\) was')" = 0)" "ни одной строки об отказе из-за пулера"
  check "$(passes is "$(st 1001 'int(d["maintenanceSinceSuccessSeconds"] or 999) < 10')" = True)" "обслуживание идёт через пулер: since_success < 10 с"
  check "$(passes effects_grow 5)" "доставка идёт"
  # a session lock or setting left behind would stay in an idle server session: none may hold an advisory lock
  check "$(passes is "$(psql_q "select count(*) from pg_locks l join pg_stat_activity a using (pid) where l.locktype = 'advisory' and a.state = 'idle'")" = 0)" "в простаивающих серверных сессиях нет advisory-блокировок"
  log "premade $(st 1001 'd["premade"]'), since_success $(st 1001 'd["maintenanceSinceSuccessSeconds"]')"
  pc_finish
}
