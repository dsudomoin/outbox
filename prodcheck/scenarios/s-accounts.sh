# One queue per account: a service that uses the outbox as a durable input queue, with a Kora transaction of its own in
# the handler. As such a service runs: Postgres 17, DML role and owner role with migrations (bootstrap off), one relay
# worker, library defaults everywhere else, protobuf envelope, handleBatch only (handle throws), requeue on a business
# block, a row lock on the account inside the handler, no metrics, no admin API, no hook.
ACC=account-events
ACC_LOAD='rate=80&writers=4&keys=30&tags=block:97,unblock:101,wait:7&rollbackPercent=3'
# the handler as the service has it: a later record of a key may pass a parked one when the block ends inside a call
ACC_FINISH=(--exempt-tags wait --report-order-within-tag wait --timeout 120)
acc_q() { psql_q "set search_path = accounts; $1"; }

acc_begin() { # acc_begin NAME: Postgres 17, the roles, the migration as the owner, the ledger in the app's schema
  pc_begin "$1"
  docker stop -t 2 "$PG" > /dev/null
  source_replace "" 55432 postgres:17
  docker exec "$PG" psql -U outbox -d postgres -Atq -c "drop database if exists outbox with (force)" -c "create database outbox"
  docker exec -i "$PG" psql -U outbox -d outbox -q -v ON_ERROR_STOP=1 < "$PC/accounts/roles.sql"
  docker exec -i "$PG" psql -U accounts_owner -d outbox -q -v ON_ERROR_STOP=1 < "$PC/accounts/schema.sql"
  docker exec -i -e PGOPTIONS='-c search_path=accounts' "$PG" psql -U accounts_owner -d outbox -q -v ON_ERROR_STOP=1 < "$PC/scenarios/ledger.sql"
  acc_q "insert into subscription (account, id) select 'k' || g, gen_random_uuid() from generate_series(0, 29) g" > /dev/null
  [ -n "${NO_GRANT:-}" ] || docker exec -i "$PG" psql -U accounts_owner -d outbox -q -v ON_ERROR_STOP=1 < "$PC/accounts/grant-sequence.sql"
  BASES=(accounts.account_events)
  LEDGER_ARGS=(--search-path accounts,public)
  EFFECTS_TABLE=accounts.pc_effect
  NO_ADMIN=1
}

acc() { # acc I [-Dkey=value...]: (re)starts instance I; its load comes back with it
  local i=$1
  shift
  APP=$ACCOUNTS inst_start "$i" "pc-$SCN-$i" POSTGRES_URL="$PC_PG_URL" INSTANCE_NAME="app-$i" "JAVA_OPTS=-Xmx768m $*"
  if [ -n "${LOADS[$i]:-}" ]; then pc_load "$i" "${LOADS[$i]}"; fi
}

blocked_accounts() { acc_q "select account from blocker"; }

unblock_all() { # one unblock per blocked account: what ends the business wait in the service
  local i=$1 a
  for a in $(blocked_accounts); do tx_send "$i" "topic=$ACC&key=$a&tag=unblock" > /dev/null; done
}

acc_checks() { # what the library must keep true whatever happened
  check "$(passes is "$(acc_q 'select count(*) from handle_called')" = 0)" "handle() не вызван ни разу: всё через handleBatch"
  local m
  m=$(acc_q 'select count(*) from mismatch')
  check "$(passes is "$m" = 0)" "id, message_id, created_at, ключ, payload и заголовки при повторах не менялись, счётчики не убывали (расхождений $m)"
}

acc_end() { # stop the writers, clear the blocks, then the usual reconciliation
  local i
  for i in "${!APP_PIDS[@]}"; do if [ "$i" -lt "$T" ] && up "$i"; then pc_unload "$i"; alive_i=$i; fi; done
  unblock_all "$alive_i"
  sleep 3
  unblock_all "$alive_i"
  acc_checks
  pc_finish "${ACC_FINISH[@]}" "$@"
}

acc_load() { pc_load "$1" "${2:-$ACC_LOAD}"; }

s_accounts_start_check() { # the migration without SELECT on the sequence: a refusal that says what is missing; a grant repairs it
  NO_GRANT=1 acc_begin accounts-start-check
  local log=$LOGS/pc-$SCN-1001.log rc=0
  bounded 60 env HTTP_PORT=9080 METRICS_PORT=9085 POSTGRES_URL="$PC_PG_URL" INSTANCE_NAME=app-1001 "JAVA_OPTS=-Xmx768m" "$ACCOUNTS" > "$log" 2>&1 || rc=$?
  check "$(passes gt "$rc" 0)" "без SELECT на последовательность старт падает (код $rc)"
  check "$(passes grep -q 'lacks SELECT on sequence' "$log")" "отказ называет право: $(grep -m1 'lacks SELECT' "$log" | cut -c1-170)"
  docker exec -i "$PG" psql -U accounts_owner -d outbox -q -v ON_ERROR_STOP=1 < "$PC/accounts/grant-sequence.sql"
  acc 1001
  acc 1002
  local warn
  warn=$(grep -hE ' (WARN|ERROR) ' "$LOGS/pc-$SCN-1001.log" "$LOGS/pc-$SCN-1002.log" | grep -v 'lacks SELECT' | cut -c1-200 | sort -u | head -5)
  log "start warnings: $warn"
  local owner
  owner=$(psql_q "select distinct pg_get_userbyid(c.relowner) from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = 'accounts.account_events'::regclass")
  check "$(passes is "$owner" = accounts_owner)" "партиции создаёт и держит роль-владелец: $owner"
  local out
  out=$(tx_send 1001 'topic=typo&key=k1')
  check "$(passes grep -q IllegalArgumentException <<< "$out")" "неизвестный топик отвергнут до записи"
  acc_load 1001
  acc_load 1002
  sleep 25
  check "$(passes gt "$(acc_q 'select coalesce(max(max_requeues), 0) from seen')" 0)" "requeue действительно идёт"
  check "$(passes gt "$(acc_q 'select count(*) from state')" 0)" "изменения бизнес-транзакции зафиксированы и при REQUEUE"
  REPAIRED="GRANT и рестарт, без кода"
  acc_end
}

s_accounts_kill() { # kill -9 while a call is in flight and records are parked; the business commit went, the relay's did not
  acc_begin accounts-kill
  local n i
  for i in 1001 1002 1003; do acc "$i"; acc_load "$i"; done
  for n in 1 2 3 4; do
    sleep 8
    i=$((1001 + n % 3))
    pc_kill "$i"
    sleep 4
    acc "$i"
  done
  sleep 5
  acc_end --min-dups 1
}

s_accounts_conn_kill() { # the backends die under the step: the failure of the relay's commit right after a business commit
  acc_begin accounts-conn-kill
  local n i
  for i in 1001 1002; do acc "$i"; acc_load "$i"; done
  for n in 1 2 3 4 5; do
    sleep 6
    fault "pg_terminate_backend всех соединений"
    psql_q "select count(pg_terminate_backend(pid)) from pg_stat_activity where application_name like 'app-%'" > /dev/null
  done
  sleep 6
  acc_end --min-dups 1
}

row_lock() { # row_lock SECONDS: someone holds the account's row as a synchronous flow of the service does
  docker exec -d "$PG" psql -U outbox -d outbox -Atq -c "begin; select 1 from accounts.subscription where account = 'k3' for update; select pg_sleep($1); commit" > /dev/null
  fault "строка счёта k3 заблокирована на $1 с"
}

s_accounts_row_lock() { # the handler waits for an account's row past the batch budget, with the service's socketTimeout = batchTimeout
  acc_begin accounts-row-lock
  local i
  for i in 1001 1002; do acc "$i"; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 5
  local from=$(now)
  row_lock 70
  sleep 75
  local to=$(now)
  sleep 5
  log "see latency of the other accounts in the result"
  acc_end --slow-keys k3 --slow-from "$from" --slow-to "$to"
}

s_accounts_row_lock_fixed() { # the same with lock_timeout below the budget: the wait ends in a Retry of one key, the worker stays free
  acc_begin accounts-row-lock-fixed
  local i
  for i in 1001 1002; do acc "$i" -Daccounts.lockTimeoutMs=2000; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 5
  local from=$(now)
  row_lock 70
  sleep 75
  local to=$(now)
  sleep 5
  acc_end --slow-keys k3 --slow-from "$from" --slow-to "$to"
}

s_accounts_zombie() { # the JDBC wait outlives the hard timeout: the abandoned call commits after its retry began; one effect
  acc_begin accounts-zombie
  local i
  for i in 1001 1002; do acc "$i" -Ddb.dsProperties.socketTimeout=120; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 5
  row_lock 45 # past batchTimeout 30 s, inside the socket timeout: the wait ignores the interrupt, the call goes on
  sleep 55
  acc_end --slow-keys k3
}

s_accounts_corrupt() { # a stored envelope that does not parse: the handler answers Retry for ever, its key stands, others go on
  acc_begin accounts-corrupt
  local i
  for i in 1001 1002; do acc "$i"; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 3
  tx_send 1001 "topic=$ACC&key=k5&tag=corrupt&raw=garbage" > /dev/null
  fault "в очереди запись, которая не разбирается"
  sleep 20
  local stuck
  stuck=$(psql_q "select count(*) from accounts.account_events_retry where convert_from(key, 'UTF8') = 'k5'")
  check "$(passes gt "$stuck" 0)" "ключ стоит в retry-таблице: $stuck записей"
  check "$(passes effects_grow 5)" "остальные ключи идут"
  # the repair at hand with no admin API: a rolling restart with maxAttempts turns it into a dead letter, its key goes on
  for i in 1001 1002; do
    inst_stop "$i"
    acc "$i" -Doutbox.relay.retry.maxAttempts=3
    sleep 3
  done
  for i in 1001 1002; do pc_unload "$i"; done
  dead_now() { [ "$(psql_q 'select count(*) from accounts.account_events_dead')" -gt 0 ]; }
  check "$(passes wait_for 90 dead_now)" "запись ушла в dead-таблицу, ключ пошёл дальше"
  REPAIRED="rolling-рестарт с maxAttempts; письмо осталось в dead: без админ-API разобрать его нечем"
  acc_checks
  pc_finish --dead-tags corrupt --exempt-tags corrupt --timeout 120
}

s_accounts_requeue_storm() { # thousands of records parked with maxRequeues = 0 for a minute: what it costs the other accounts
  acc_begin accounts-requeue-storm
  local i a
  for i in 1001 1002; do acc "$i"; done
  for a in 0 1 2 3 4; do tx_send 1001 "topic=$ACC&key=k$a&tag=block" > /dev/null; done
  sleep 2
  pc_load 1001 'rate=400&writers=4&keys=5&tags=wait:1' # every record of the blocked accounts waits, nothing else there
  sleep 15
  pc_unload 1001
  local parked
  parked=$(psql_q 'select count(*) from accounts.account_events_parked')
  check "$(passes gt "$parked" 500)" "отложено записей: $parked"
  local from=$(now)
  pc_load 1002 'rate=50&writers=2&keys=30'
  # keys k5..k29 are free; k0..k4 would be parked behind nothing: their normal records go by
  sleep 45
  local to=$(now)
  pc_unload 1002
  log "max requeues so far: $(acc_q 'select max(max_requeues) from seen')"
  ACC_FINISH=(--exempt-tags wait --report-order-within-tag wait --timeout 200 --slow-tags wait --slow-from "$from" --slow-to "$to")
  acc_end
}

s_accounts_pool_pressure() { # writers hold the whole pool of 10: the handler's business transaction and the relay compete for it
  acc_begin accounts-pool-pressure
  local i
  for i in 1001 1002; do acc "$i"; acc_load "$i" 'rate=100&writers=12&keys=30&holdMs=100'; done
  sleep 30
  local live
  live=$(psql_q "select count(*) from accounts.account_events_instance where expires_at > now()")
  check "$(passes is "$live" = 2)" "heartbeat жив при занятом пуле: $live из 2"
  log "stats: $(stats 1001)"
  acc_end
}

s_accounts_owner_down() { # the owner role's connections are refused for 30 s: delivery goes on, maintenance waits, then catches up
  acc_begin accounts-owner-down
  local i
  for i in 1001 1002; do acc "$i"; acc_load "$i"; done
  sleep 8
  psql_q "alter role accounts_owner nologin" > /dev/null
  psql_q "select count(pg_terminate_backend(pid)) from pg_stat_activity where usename = 'accounts_owner'" > /dev/null
  fault "роль-владелец не может подключиться"
  check "$(passes effects_grow 20)" "доставка идёт без роли-владельца"
  sleep 10
  psql_q "alter role accounts_owner login" > /dev/null
  fault "роль-владелец снова может"
  local before
  before=$(psql_q "select extract(epoch from succeeded_at) from accounts.account_events_maintenance")
  maintained() { [ "$(psql_q "select extract(epoch from succeeded_at) > $before from accounts.account_events_maintenance")" = t ]; }
  check "$(passes wait_for 90 maintained)" "обслуживание снова проходит само"
  acc_end
}

s_accounts_rolling() { # rolling restart under load, with records parked: graceful stop inside the defaults, parked records stay
  acc_begin accounts-rolling
  local i took ok=0 bound=$((30 + 8))
  for i in 1001 1002 1003; do acc "$i"; acc_load "$i"; done
  sleep 10
  for i in 1001 1002 1003; do
    took=$(pc_term "$i")
    [ "$took" -le "$bound" ] || ok=1
    fault "SIGTERM экземпляра $i: остановка $took с"
    acc "$i"
    sleep 6
  done
  check "$ok" "каждая остановка не дольше shutdownTimeout + 8 с"
  acc_end # every requeue is an arrival: no bound on duplicates here
}

s_accounts_pg_restart() { # Postgres 17 restarts and goes away for a minute: with default backoff (maxDelay 5 min) how soon delivery returns
  acc_begin accounts-pg-restart
  local i
  for i in 1001 1002; do acc "$i"; acc_load "$i"; done
  sleep 8
  docker restart -t 5 "$PG" > /dev/null
  fault "docker restart"
  sleep 12
  docker stop -t 5 "$PG" > /dev/null
  fault "база недоступна 60 с"
  sleep 60
  docker start "$PG" > /dev/null
  local t=$(now) before
  wait_for 30 docker exec "$PG" pg_isready -U outbox -d outbox -q || true
  before=$(effects)
  check "$(passes wait_for 120 sh -c "[ \"\$(docker exec $PG psql -U outbox -d outbox -Atq -c 'select count(*) from accounts.pc_effect')\" -gt $before ]")" "доставка вернулась сама через $(($(now) - t)) с после возврата базы"
  sleep 5
  acc_end
}

s_accounts_same_type_held() { # with the README's rule in the handler (hold a key's later records of the same type): parked rows of a key stay in order
  acc_begin accounts-same-type-held
  ACC_FINISH=(--exempt-tags wait --order-within-tag wait --timeout 120)
  local n i
  for i in 1001 1002 1003; do acc "$i" -Daccounts.holdSameType=true; acc_load "$i"; done
  for n in 1 2 3 4; do
    sleep 8
    i=$((1001 + n % 3))
    pc_kill "$i"
    sleep 4
    acc "$i" -Daccounts.holdSameType=true
  done
  sleep 5
  acc_end --min-dups 1
}
