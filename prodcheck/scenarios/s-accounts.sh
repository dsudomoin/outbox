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
  AFTER=mismatch_dump
}

mismatch_dump() { # on the way out, the database still there: everything that explains a mismatch, none of it cut
  local m
  m=$(acc_q 'select count(*) from mismatch') || m=0
  [ "$m" != 0 ] || return 0
  psql_q 'copy accounts.mismatch to stdout with csv header' > "$RESULT_DIR/$SCN.mismatch"
  psql_q 'copy accounts.seen to stdout with csv header' > "$RESULT_DIR/$SCN.seen"
  psql_q 'copy (select * from accounts.pc_arrival where message_id in (select message_id from accounts.mismatch) order by no) to stdout with csv header' > "$RESULT_DIR/$SCN.arrivals"
  cp "$LOGS/pc-$SCN-"*.log "$RESULT_DIR/"
}

acc() { # acc I [-Dkey=value...]: (re)starts instance I; its load comes back with it
  local i=$1
  shift
  APP=${ACC_APP:-$ACCOUNTS} inst_start "$i" "pc-$SCN-$i" POSTGRES_URL="$PC_PG_URL" POSTGRES_OWNER_URL="${ACC_OWNER_URL:-$PC_PG_URL}" INSTANCE_NAME="app-$i" "JAVA_OPTS=-Xmx768m $*"
  if [ -n "${LOADS[$i]:-}" ]; then pc_load "$i" "${LOADS[$i]}"; fi
}

acc_full() { # the build with metrics and the admin API: /status stands for the ledger's counts
  ACC_APP=$ACCOUNTS_FULL
  NO_ADMIN=""
}

blocked_accounts() { acc_q "select account from blocker"; }

unblock_all() { # one unblock per blocked account: what ends the business wait in the service
  local i=$1 a
  for a in $(blocked_accounts); do tx_send "$i" "topic=$ACC&key=$a&tag=unblock" > /dev/null; done
}

acc_checks() { # what the library must keep true whatever happened
  check "$(passes is "$(acc_q 'select count(*) from handle_called')" = 0)" "handle() не вызван ни разу: всё через handleBatch"
  local m lost=0
  m=$(acc_q 'select count(*) from mismatch')
  [ "$m" = 0 ] || log "mismatches: $(acc_q "select message_id, left(reason, 160) from mismatch limit 20")"
  # a step that did not commit takes back the requeues it counted, a row offered twice in it shows the lower count next:
  # ShardProcessorIT.aLostStepTakesBackTheRequeuesItCounted. So in a run with faults, and by no more than such a step counts.
  [ -z "$FAULTS" ] || lost=$(acc_q "select count(*) from (select regexp_match(reason, '^requeues went down from ([0-9]+) to ([0-9]+)$') r from mismatch) x where r[1]::int - r[2]::int <= 2")
  m=$((m - lost))
  check "$(passes is "$m" = 0)" "id, message_id, created_at, ключ, payload и заголовки при повторах не менялись, счётчики убывали только с потерянным шагом (расхождений $m, таких шагов $lost)"
}

acc_end() { # stop the writers, clear the blocks, then the usual reconciliation
  local i
  for i in "${!APP_PIDS[@]}"; do if [ "$i" -lt "$T" ] && up "$i"; then pc_unload "$i"; alive_i=$i; fi; done
  unblock_all "$alive_i"
  sleep 3
  unblock_all "$alive_i"
  AFTER_LEDGER=acc_checks # a record's last redelivery may come in the drain
  pc_finish "${ACC_FINISH[@]}" "$@"
}

acc_load() { pc_load "$1" "${2:-$ACC_LOAD}"; }

parked_watch() { # how many rows each key had parked, every 2 s; on the way out each overtaking in a type next to that number
  ( while :; do acc_q "select extract(epoch from now()), convert_from(key, 'UTF8'), count(*) from account_events_parked group by 2" 2> /dev/null || true; command sleep 2; done ) > "$RESULT_DIR/$SCN.parked" &
  PARKED_WATCH=$!
  AFTER="$AFTER; parked_report"
}

parked_report() {
  kill "$PARKED_WATCH" 2> /dev/null || true
  acc_q "copy (select key, tag, seq, extract(epoch from at) from pc_effect where tag = 'wait' order by no) to stdout with csv" | python3 -c "
import collections, csv, sys
samples = collections.defaultdict(list)
for l in open(sys.argv[1]):
    p = l.strip().split('|')
    if len(p) == 3:
        samples[p[1]].append((float(p[0]), int(p[2])))
top, out = {}, []
for key, tag, seq, at in csv.reader(sys.stdin):
    seq, at = int(seq), float(at)
    if key in top and seq < top[key][0]:
        near = [n for t, n in samples[key] if t <= at]
        out.append(f'{key}: seq {seq} after {top[key][0]}, parked on the key at the last sample before it {near[-1] if near else 0}, largest so far {max(near) if near else 0}')
    if key not in top or seq > top[key][0]:
        top[key] = (seq, at)
print(f'overtakings within wait: {len(out)}; largest parked per key in the run: {max((n for v in samples.values() for _, n in v), default=0)}')
print('\\n'.join(out[:40]))
" "$RESULT_DIR/$SCN.parked" > "$RESULT_DIR/$SCN.inversions" 2>&1
}

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

requeue_storm() { # requeue_storm NAME [-Dkey=value...]: thousands of records parked with maxRequeues = 0 for a minute: what it costs the other accounts
  acc_begin "$1"
  shift
  parked_watch
  local i a
  for i in 1001 1002; do acc "$i" "$@"; done
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

s_accounts_requeue_storm() { requeue_storm accounts-requeue-storm; }

s_accounts_requeue_storm_delay() { # the same with a longer requeue delay: arrivals and the others' p99 next to the default's
  requeue_storm accounts-requeue-storm-delay -Doutbox.relay.requeue.delay=5s
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
  parked_watch
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

# The service's recommendations, each with the plain form it replaces. The defaults stay the service's.
RECOMMENDED='-Daccounts.lockTimeoutMs=2000 -Daccounts.holdSameType=true -Daccounts.unknownSource=wait -Daccounts.corruptPermanent=true'
READS_NEW='-Daccounts.knownSources=0,99'

s_accounts_unknown_source_ack() { # as the service has it: a source it does not know is acknowledged, the event is gone with no effect
  acc_begin accounts-unknown-source-ack
  local i n
  for i in 1001 1002; do acc "$i"; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 3
  for n in 1 2 3 4 5; do tx_send 1001 "topic=$ACC&key=k7&tag=unknown-source" > /dev/null; done
  fault "пять событий с неизвестным номером source"
  sleep 8
  local arrived applied dead left
  arrived=$(acc_q "select count(*) from pc_arrival where tag = 'unknown-source'")
  applied=$(acc_q "select count(*) from pc_effect where tag = 'unknown-source'")
  dead=$(acc_q "select count(*) from account_events_dead")
  left=$(acc_q "select (select count(*) from account_events_retry) + (select count(*) from account_events_parked where convert_from(key, 'UTF8') = 'k7' and false)")
  check "$(passes is "$arrived" = 5)" "все пять дошли до обработчика"
  check "$(passes is "$applied" = 0)" "эффекта нет: событие подтверждено и исчезло, потеря по коду сервиса ($applied эффектов)"
  check "$(passes is "$dead" = 0)" "и следов нет: в dead $dead, ждущих $left"
  REPAIRED="ничего вернуть нельзя: так устроен ACK неизвестного source в сервисе; лечит accounts-unknown-source-wait"
  acc_end --discard-tags unknown-source --exempt-tags wait,unknown-source
}

s_accounts_unknown_source_wait() { # the recommendation: wait, no attempt spent, other keys flow, a release that reads it delivers, in order
  acc_begin accounts-unknown-source-wait
  local i n opts=-Daccounts.unknownSource=wait
  for i in 1001 1002; do acc "$i" $opts; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 3
  for n in 1 2 3; do tx_send 1001 "topic=$ACC&key=k7&tag=unknown-source" > /dev/null; done
  fault "три события с неизвестным номером source на счёте k7"
  sleep 6
  check "$(passes is "$(acc_q "select count(*) from account_events_retry where waiting_since is not null and convert_from(key, 'UTF8') = 'k7'")" = 1)" "ключ ждёт: одна ждущая голова"
  sleep 12
  check "$(passes is "$(acc_q "select coalesce(max(attempts), 0) from account_events_retry where waiting_since is not null")" = 0)" "попытки не тратятся"
  check "$(passes is "$(acc_q "select count(*) from pc_effect where tag = 'unknown-source'")" = 0)" "эффекта пока нет"
  check "$(passes effects_grow 5)" "остальные ключи идут"
  local behind
  behind=$(acc_q "select count(*) from account_events_retry where convert_from(key, 'UTF8') = 'k7'")
  log "k7 holds $behind records behind its waiting head"
  fault "релиз, который читает этот source: rolling-рестарт"
  for i in 1001 1002; do
    inst_stop "$i"
    acc "$i" $opts $READS_NEW
    sleep 3
  done
  REPAIRED="выкатка релиза, который читает новый source"
  acc_end
}

s_accounts_corrupt_fixed() { # the recommendation: an envelope that never parses is a letter at once, the key moves on, an operator decides
  acc_begin accounts-corrupt-fixed
  acc_full
  local i opts=-Daccounts.corruptPermanent=true
  for i in 1001 1002; do acc "$i" $opts; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 3
  local id t
  id=$(tx_send 1001 "topic=$ACC&key=k5&tag=corrupt&raw=garbage" | j 'd["id"]')
  t=$(now)
  fault "в очереди запись, которая не разбирается: $id"
  dead_now() { [ "$(acc_q "select count(*) from account_events_dead")" = 1 ]; }
  check "$(passes wait_for 10 dead_now)" "сразу в dead: через $(($(now) - t)) с"
  tx_send 1001 "topic=$ACC&key=k5&tag=after" > /dev/null
  after() { [ "$(acc_q "select count(*) from pc_effect where tag = 'after'")" = 1 ]; }
  check "$(passes wait_for 10 after)" "следующее событие счёта прошло: ключ идёт дальше"
  check "$(passes is "$(acc_q "select count(*) from account_events_retry")" = 0)" "в retry ничего не стоит"
  local letter
  letter=$(admin_body 1001 GET "dead-letters?topic=$ACC&limit=10")
  check "$(passes grep -q "$id" <<< "$letter")" "письмо видно в админ-API с причиной: $(echo "$letter" | j 'd["items"][0]["error"][:110]')"
  dead_gauge() { [ "$(on 1001 metric outbox_dead_size)" -gt 0 ]; }
  check "$(passes wait_for 40 dead_gauge)" "и в метрике outbox.dead.size: обслуживание обновляет её раз в maintenanceInterval"
  local letter_id
  letter_id=$(echo "$letter" | j 'd["items"][0]["id"]')
  local code
  code=$(admin_code 1001 POST dead-letters/redrive "{\"ids\":[$letter_id]}")
  check "$(passes is "$code" = 200)" "redrive принят: $code"
  died_again() { [ "$(acc_q "select redrives from account_events_dead where message_id = '$id'")" = 1 ]; }
  check "$(passes wait_for 15 died_again)" "снова в dead, redrives = 1: запись та же, разбирать по-прежнему нечего"
  letter_id=$(acc_q "select id from account_events_dead where message_id = '$id'")
  code=$(admin_code 1001 POST dead-letters/discard "{\"ids\":[$letter_id]}")
  check "$(passes is "$code" = 200)" "discard убирает письмо: $code"
  local d=$LOGS/pc-$SCN-discarded.txt
  echo "$id" > "$d"
  REPAIRED="оператор решает через админ-API"
  ACC_FINISH=(--exempt-tags wait --report-order-within-tag wait --timeout 120 --discarded-file "$d")
  acc_end
}

s_accounts_corrupt_abandon() { # the service's own form: Retry for ever on an envelope that does not parse; one admin call instead of a rolling restart
  acc_begin accounts-corrupt-abandon
  acc_full # the build with the admin API, every option as the service has it
  local i pids
  for i in 1001 1002; do acc "$i"; acc_load "$i" 'rate=50&writers=2&keys=30'; done
  sleep 3
  tx_send 1001 "topic=$ACC&key=k5&tag=corrupt&raw=garbage" > /dev/null
  fault "в очереди запись, которая не разбирается"
  sleep 15
  local stuck
  stuck=$(acc_q "select count(*) from account_events_retry where convert_from(key, 'UTF8') = 'k5'")
  check "$(passes gt "$stuck" 0)" "ключ стоит в retry-таблице: $stuck записей"
  check "$(passes is "$(acc_q 'select count(*) from account_events_dead')" = 0)" "сама запись в dead не уходит: maxAttempts = 0"
  pids="${APP_PIDS[1001]} ${APP_PIDS[1002]}"
  local body
  body=$(admin_body 1001 POST keys/abandon "{\"topic\":\"$ACC\",\"key\":\"k5\",\"reason\":\"an envelope that does not parse\"}")
  fault "POST keys/abandon: $body"
  check "$(passes is "$(echo "$body" | j 'd["records"] > 0')" = True)" "ключ снят одним вызовом: $body"
  check "$(passes is "$(acc_q "select count(*) from account_events_retry where convert_from(key, 'UTF8') = 'k5' and id <= (select max(id) from account_events_dead)")" = 0)" "снятых записей в retry не осталось"
  tx_send 1001 "topic=$ACC&key=k5&tag=after" > /dev/null
  after() { [ "$(acc_q "select count(*) from pc_effect where tag = 'after'")" = 1 ]; }
  check "$(passes wait_for 30 after)" "следующее событие счёта прошло"
  check "$(passes is "$pids" = "${APP_PIDS[1001]} ${APP_PIDS[1002]}")" "те же процессы: без рестарта и смены конфига"
  REPAIRED="POST keys/abandon на живом инстансе; письма остаются у оператора"
  ACC_FINISH=(--exempt-tags wait,corrupt,normal --dead-tags corrupt,normal --timeout 120)
  acc_end
}

s_accounts_recommended() { # everything on: kill -9, a row lock, a corrupt envelope, an unknown source, Postgres restart
  acc_begin accounts-recommended
  parked_watch
  acc_full
  local i n
  for i in 1001 1002 1003; do acc "$i" $RECOMMENDED; acc_load "$i"; done
  sleep 8
  pc_kill 1001
  sleep 3
  acc 1001 $RECOMMENDED
  sleep 3
  local from=$(now)
  row_lock 20
  tx_send 1002 "topic=$ACC&key=k5&tag=corrupt&raw=garbage" > /dev/null
  tx_send 1002 "topic=$ACC&key=k7&tag=unknown-source" > /dev/null
  fault "конверт не разбирается на k5, неизвестный source на k7"
  sleep 22
  local to=$(now)
  local status
  status=$(admin_body 1002 GET status)
  log "status: $(echo "$status" | j '{k: d[k] for k in ("lagSeconds", "retrySize", "parkedSize", "waitingSize", "deadLetters")}')"
  check "$(passes is "$(echo "$status" | j 'd["waitingSize"]')" = 1)" "/status показывает ждущую запись"
  check "$(passes grep -q "$ACC" <<< "$(echo "$status" | j 'd["deadLetters"]')")" "/status показывает письмо"
  check "$(passes is "$(on 1002 metric outbox_waiting_size)" = 1)" "метрика outbox.waiting.size = 1"
  check "$(passes gt "$(on 1002 metric outbox_dead_size)" 0)" "метрика outbox.dead.size > 0"
  check "$(passes gt "$(on 1002 metric outbox_messages_requeued_total)" 0)" "метрика outbox.messages.requeued растёт"
  docker restart -t 5 "$PG" > /dev/null
  fault "docker restart Postgres"
  sleep 15
  local before back
  before=$(acc_q "select count(*) from pc_effect")
  back() { [ "$(acc_q "select count(*) from pc_effect")" -gt "$before" ]; }
  check "$(passes wait_for 120 back)" "доставка вернулась сама после рестарта Postgres"
  fault "релиз, который читает новый source: rolling-рестарт"
  for i in 1001 1002 1003; do
    inst_stop "$i"
    acc "$i" $RECOMMENDED $READS_NEW
    sleep 3
  done
  ACC_FINISH=(--exempt-tags wait,corrupt --order-within-tag wait --dead-tags corrupt --timeout 180 --slow-keys k3,k5,k7 --slow-tags wait --slow-from "$from" --slow-to "$to" --p99-max 10)
  acc_end
}

s_accounts_rollback_drain() { # the service's rollback in its production form: the queue may go only when /status says there is nothing left
  acc_begin accounts-rollback-drain
  acc_full
  local i opts='-Daccounts.unknownSource=wait'
  for i in 1001 1002; do acc "$i" $opts; acc_load "$i" 'rate=50&writers=2&keys=30&tags=wait:7&rollbackPercent=3'; done
  sleep 3
  # a record that requeues for ever (its account's block never ends, maxRequeues = 0), and one no release reads
  tx_send 1001 "topic=$ACC&key=k9&tag=block" > /dev/null
  tx_send 1001 "topic=$ACC&key=k11&tag=unknown-source" > /dev/null
  sleep 8
  fault "приём остановлен"
  for i in 1001 1002; do pc_unload "$i"; done
  # the procedure: every other wait ends by itself; these two do not
  local empty='d["lagSeconds"] == 0 and d["retrySize"] == 0 and d["parkedSize"] == 0 and d["waitingSize"] == 0'
  sleep 20
  local status
  status=$(admin_body 1001 GET status)
  log "20 s after the intake stopped: $(echo "$status" | j '{k: d[k] for k in ("lagSeconds", "retrySize", "parkedSize", "waitingSize", "deadLetters")}')"
  check "$(passes is "$(echo "$status" | j "$empty")" = False)" "сама очередь не пустеет: запись ждёт релиз, запись откладывается вечно; /status это показывает, удалять нельзя"
  check "$(passes gt "$(echo "$status" | j 'd["parkedSize"]')" 0)" "отложенные видны: $(echo "$status" | j 'd["parkedSize"]')"
  check "$(passes gt "$(echo "$status" | j 'd["waitingSize"]')" 0)" "ждущие видны: $(echo "$status" | j 'd["waitingSize"]')"
  # one call each, on the live instances: what waits, and what requeues for ever
  local body pids="${APP_PIDS[1001]} ${APP_PIDS[1002]}"
  body=$(admin_body 1001 POST waiting/abandon "{\"topic\":\"$ACC\",\"reason\":\"rollback of the release\",\"dryRun\":false}")
  log "waiting/abandon: $body"
  check "$(passes is "$(echo "$body" | j 'd["records"] > 0')" = True)" "waiting/abandon перенёс ждущие в dead: $body"
  body=$(admin_body 1001 POST keys/abandon "{\"topic\":\"$ACC\",\"key\":\"k9\",\"reason\":\"rollback of the release\"}")
  log "keys/abandon: $body"
  check "$(passes is "$(echo "$body" | j 'd["records"] > 0')" = True)" "keys/abandon снял вечно откладываемый счёт: $body"
  check "$(passes is "$pids" = "${APP_PIDS[1001]} ${APP_PIDS[1002]}")" "те же процессы: без рестарта и смены конфига"
  emptied() { [ "$(admin_body 1001 GET status | j "$empty")" = True ]; }
  local t=$(now)
  check "$(passes wait_for 90 emptied)" "/status пуст через $(($(now) - t)) с: lag, retry, parked, waiting по нулям"
  # that very moment: everything committed is either applied or a letter an operator holds
  local undelivered letters
  undelivered=$(acc_q "select count(*) from pc_sent s where not exists (select 1 from pc_effect e where e.message_id = s.message_id) and not exists (select 1 from account_events_dead d where d.message_id = s.message_id)")
  letters=$(acc_q "select count(*) from account_events_dead")
  check "$(passes is "$undelivered" = 0)" "в этот момент недоставленных нет: $undelivered; писем у оператора $letters: их выгружают до удаления очереди"
  check "$(passes gt "$letters" 0)" "письма на месте: $letters"
  REPAIRED="процедура: waiting/abandon для ждущих, keys/abandon для вечно отложенных, затем письма выгружают"
  # the abandoned key takes the records behind its head along, whatever their tag; the block's key only its own
  local others
  others=$(acc_q "select count(*) from account_events_dead where convert_from(key, 'UTF8') not in ('k9', 'k11')")
  check "$(passes is "$others" = 0)" "письма только этих двух счетов: чужих $others"
  ACC_FINISH=(--exempt-tags wait,unknown-source,normal --dead-tags wait,unknown-source,normal --timeout 60)
  log "blocked accounts left: $(acc_q 'select count(*) from blocker')"
  acc_checks
  pc_finish "${ACC_FINISH[@]}"
}

s_accounts_held_deadlock() { # the README's same-type rule against a later event of the type that carries the awaited result: the key stands
  acc_begin accounts-held-deadlock
  local i opts=-Daccounts.holdSameType=true
  for i in 1001 1002; do acc "$i" $opts; acc_load "$i" 'rate=50&writers=2&keys=25'; done # k27 is left to the fault
  tx_send 1001 "topic=$ACC&key=k27&tag=block" > /dev/null
  sleep 2
  tx_send 1001 "topic=$ACC&key=k27&tag=wait" > /dev/null
  tx_send 1001 "topic=$ACC&key=k27&tag=wait" > /dev/null
  tx_send 1001 "topic=$ACC&key=k27&tag=wait%2Bresult" > /dev/null
  fault "счёт k27 заблокирован; две записи wait ждут результата, третья того же типа его несёт"
  requeues() { acc_q "select coalesce(max(requeues), 0) from account_events_parked where convert_from(key, 'UTF8') = 'k27'"; }
  local a b
  sleep 15
  a=$(requeues)
  sleep 45
  b=$(requeues)
  log "after 60 s: requeues $a -> $b, parked $(acc_q "select count(*) from account_events_parked where convert_from(key, 'UTF8') = 'k27'")"
  check "$(passes gt "$b" "$a")" "ключ крутится вхолостую: откладываний было $a, через 45 с $b"
  check "$(passes is "$(acc_q "select count(*) from account_events_parked where convert_from(key, 'UTF8') = 'k27'")" = 3)" "ключ стоит: все три записи отложены"
  check "$(passes is "$(acc_q "select count(*) from pc_arrival where tag = 'wait+result'")" = 0)" "запись с результатом ни разу не дошла до бизнес-операции: правило heldTypes держит её в обработчике"
  check "$(passes is "$(acc_q "select count(*) from blocker where account = 'k27'")" = 1)" "блокировка на месте: снять её некому, дедлок ключа"
  check "$(passes is "$(acc_q "select count(*) from pc_effect where key = 'k27' and tag <> 'block'")" = 0)" "за минуту ни одного эффекта на счёте"
  check "$(passes effects_grow 5)" "другие счета идут"
  fault "rolling-рестарт без holdSameType: запись с результатом проходит мимо ждущих"
  for i in 1001 1002; do
    inst_stop "$i"
    acc "$i"
    sleep 3
  done
  gone() { [ "$(acc_q "select count(*) from account_events_parked where convert_from(key, 'UTF8') = 'k27'")" = 0 ]; }
  check "$(passes wait_for 30 gone)" "без правила ключ разошёлся сам: результат снял блокировку, ждущие доставлены"
  REPAIRED="рестарт без правила: для таких типов правило heldTypes из README неприменимо"
  ACC_FINISH=(--exempt-tags wait,wait+result --timeout 120)
  acc_end
}

# A key's records whose wait another record of the key ends, with more of them parked than one call carries. The queue must
# clear with no call from anyone, or the service's business waits end only by an operator.
parked_of() { acc_q "select count(*) from account_events_parked where convert_from(key, 'UTF8') = '$1'"; }
tail_evidence() { # tail_evidence KEY TAG: how often the older rows and the record that would end the wait reached the handler
  log "$1 parked: $(parked_of "$1"), requeues of parked rows min/max: $(acc_q "select min(requeues), max(requeues) from account_events_parked where convert_from(key, 'UTF8') = '$1'"); arrivals of $2: $(acc_q "select count(*) from pc_arrival where key = '$1' and tag = '$2'"), of wait rows by count: $(acc_q "select min(n), max(n) from (select count(*) n from pc_arrival where key = '$1' and tag = 'wait' group by message_id) x"); blockers: $(acc_q "select string_agg(account, ',') from blocker where account like '$1%'")"
}

s_accounts_tail_liveness() { # the record that ends the wait is parked far past the first chunk, and its own block was lifted later
  acc_begin accounts-tail-liveness
  local i n key=k27 opts=-Doutbox.relay.batchSize=20
  for i in 1001 1002; do acc "$i" $opts; acc_load "$i" 'rate=50&writers=2&keys=25'; done
  acc_q "insert into blocker (account) values ('$key:ender')" > /dev/null
  tx_send 1001 "topic=$ACC&key=$key&tag=block" > /dev/null
  blocked_now() { [ "$(acc_q "select count(*) from blocker where account = '$key'")" = 1 ]; }
  wait_for 20 blocked_now
  for n in $(seq 1 50); do tx_send 1001 "topic=$ACC&key=$key&tag=wait" > /dev/null; done
  tx_send 1001 "topic=$ACC&key=$key&tag=ender" > /dev/null
  for n in $(seq 1 10); do tx_send 1001 "topic=$ACC&key=$key&tag=wait" > /dev/null; done
  offered() { [ "$(acc_q "select count(*) from pc_arrival where key = '$key' and tag = 'ender'")" -ge 1 ] && [ "$(parked_of "$key")" = 61 ]; }
  check "$(passes wait_for 60 offered)" "все 61 запись отложены, запись-развязка предложена один раз и заблокирована своей блокировкой"
  tail_evidence "$key" ender
  sleep 5
  acc_q "delete from blocker where account = '$key:ender'" > /dev/null
  fault "снята блокировка записи-развязки: она стоит за первым чанком, в вызов помещается 20"
  local t=$(now)
  clears() { [ "$(parked_of "$key")" = 0 ]; }
  local verdict=0
  wait_for 120 clears || verdict=1
  tail_evidence "$key" ender
  check "$verdict" "ключ сошёлся сам за $(($(now) - t)) с из 120: запись за первым чанком предлагается снова, никто не вызывал админ-API"
  if [ "$verdict" != 0 ]; then
    thread_dump 1001 stuck
    acc_q "delete from blocker where account = '$key'" > /dev/null # only so that the rest of the ledger can be read
    REPAIRED="руками: снята блокировка счёта; сам ключ не сходится"
  fi
  ACC_FINISH=(--exempt-tags wait,ender --timeout 120)
  acc_end
}

s_accounts_tail_liveness_retry() { # a retry follower ends the wait, with more older parked rows than one call carries
  acc_begin accounts-tail-liveness-retry
  local i n key=k26 opts=-Doutbox.relay.batchSize=20
  for i in 1001 1002; do acc "$i" $opts; acc_load "$i" 'rate=50&writers=2&keys=25'; done
  tx_send 1001 "topic=$ACC&key=$key&tag=block" > /dev/null
  blocked_now() { [ "$(acc_q "select count(*) from blocker where account = '$key'")" = 1 ]; }
  wait_for 20 blocked_now
  for n in $(seq 1 50); do tx_send 1001 "topic=$ACC&key=$key&tag=wait" > /dev/null; done
  parked_all() { [ "$(parked_of "$key")" = 50 ]; }
  check "$(passes wait_for 60 parked_all)" "50 записей отложены, в вызов помещается 20"
  # the head that fails puts the key in the retry table; the record that ends the block stays behind it there
  acc_q "insert into blocker (account) values ('$key:flaky')" > /dev/null
  tx_send 1001 "topic=$ACC&key=$key&tag=flaky" > /dev/null
  tx_send 1001 "topic=$ACC&key=$key&tag=unblock" > /dev/null
  in_retry() { [ "$(acc_q "select count(*) from account_events_retry where convert_from(key, 'UTF8') = '$key'")" = 2 ]; }
  check "$(passes wait_for 60 in_retry)" "голова сбоит, запись-развязка стоит за ней в retry-таблице"
  tail_evidence "$key" unblock
  sleep 5
  acc_q "delete from blocker where account = '$key:flaky'" > /dev/null
  fault "голова больше не сбоит: дальше решает запись-развязка в retry-таблице"
  local t=$(now) verdict=0
  clears() { [ "$(parked_of "$key")" = 0 ] && [ "$(acc_q "select count(*) from account_events_retry where convert_from(key, 'UTF8') = '$key'")" = 0 ]; }
  wait_for 120 clears || verdict=1
  tail_evidence "$key" unblock
  check "$verdict" "ключ сошёлся сам за $(($(now) - t)) с из 120: запись из retry-таблицы дошла, хотя старых отложенных больше вызова"
  if [ "$verdict" != 0 ]; then
    thread_dump 1001 stuck
    acc_q "delete from blocker where account = '$key'" > /dev/null
    REPAIRED="руками: снята блокировка счёта; сам ключ не сходится"
  fi
  ACC_FINISH=(--exempt-tags wait,flaky,unblock --timeout 120)
  acc_end
}

s_parked_many_keys() { # many keys, each with a small parked tail, one shard: requeue.delay is honoured and no row is left out
  acc_begin parked-many-keys
  local i rows delay=1 window=60
  acc_q "insert into subscription (account, id) select 'k' || g, gen_random_uuid() from generate_series(30, 199) g; insert into blocker (account) select 'k' || g from generate_series(0, 199) g" > /dev/null
  local opts="-Doutbox.relay.batchSize=20 -Doutbox.relay.requeue.delay=${delay}s -Doutbox.shards=1"
  for i in 1001 1002; do acc "$i" $opts; done
  pc_load 1001 'rate=400&writers=4&keys=200&tags=wait:1'
  sleep 5
  pc_unload 1001
  rows=$(acc_q "select count(*) from pc_sent")
  all_parked() { [ "$(acc_q 'select count(*) from account_events_parked')" = "$rows" ]; }
  check "$(passes wait_for 120 all_parked)" "отложены все $rows записей: $(acc_q 'select count(distinct key) from pc_sent') ключей, один шард, в вызов помещается 20"
  # the window: every row answers requeue; nobody writes
  local upd0 upd1 from to t0
  upd0=$(psql_q "select n_tup_upd from pg_stat_user_tables where schemaname = 'accounts' and relname = 'account_events_parked'")
  from=$(psql_q "select extract(epoch from clock_timestamp())")
  t0=$(now)
  sleep "$window"
  to=$(psql_q "select extract(epoch from clock_timestamp())")
  sleep 2 # table statistics reach the view about once a second
  upd1=$(psql_q "select n_tup_upd from pg_stat_user_tables where schemaname = 'accounts' and relname = 'account_events_parked'")
  local stats ups max_row min_row offered
  stats=$(acc_q "with n as (select s.message_id, count(a.no) c from pc_sent s left join pc_arrival a on a.message_id = s.message_id and a.at >= to_timestamp($from) and a.at < to_timestamp($to) group by 1)
    select min(c), percentile_disc(0.5) within group (order by c), percentile_disc(0.99) within group (order by c), max(c), sum(c), count(*) filter (where c > 0), count(*) from n")
  IFS='|' read -r min_row p50 p99 max_row total offered all <<< "$stats"
  ups=$(((upd1 - upd0) / window))
  log "in $window s: arrivals total $total ($((total / window))/s), per row min $min_row p50 $p50 p99 $p99 max $max_row, offered $offered of $all; n_tup_upd $((upd1 - upd0)), $ups/s"
  local allowed=$((window / delay + 6)) updates_allowed=$((2 * rows / delay))
  check "$(passes is "$max_row" -le "$allowed")" "задержка соблюдается: на строку не больше $allowed прибытий за $window с при requeue.delay = $delay с; было p50 $p50, p99 $p99, max $max_row, всего $total"
  check "$(passes is "$offered" = "$all")" "за окно предложена каждая строка: $offered из $all, минимум на строку $min_row"
  check "$(passes is "$ups" -le "$updates_allowed")" "обновлений outbox_parked в секунду не больше двух на строку за задержку: $ups при пределе $updates_allowed"
  acc_q "delete from blocker" > /dev/null
  fault "блокировки сняты"
  ACC_FINISH=(--exempt-tags wait --report-order-within-tag wait --timeout 240)
  acc_end
}
