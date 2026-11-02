# Process faults: «падения приложения», «несколько инстансов», «остановка и рестарт» of the README's guarantees.
BATCH_DUPS=$((3 * 4 * 100)) # one kill: every worker's calls in flight, about 3 × batchSize each at the worst
MIX='rate=120&writers=4&keys=30&topics=payments,ledger,projection&rollbackPercent=5'

trio() { # trio LOAD [-Dkey=value...]: the target and three shops under the same load
  local load=$1 i
  shift
  pc_target
  for i in 1001 1002 1003; do pc_shop "$i" "$@"; done
  tfault 'delayMs=5'
  for i in 1001 1002 1003; do pc_load "$i" "$load"; done
}

s_steady() {
  pc_begin steady
  trio "$MIX"
  sleep 45
  local out
  out=$(curl -s -XPOST "$(pub 1001)/tx/outside")
  check "$(passes grep -q IllegalStateException <<< "$out")" "send вне транзакции бросает IllegalStateException: $out"
  out=$(tx_send 1001 'topic=typo&key=k')
  check "$(passes grep -q IllegalArgumentException <<< "$out")" "опечатка в топике падает до записи: ${out:0:120}"
  local i ok=0
  for i in 1001 1002 1003; do [ "$(on "$i" metric outbox_messages_delivered_total)" -gt 0 ] || ok=1; done
  check "$ok" "доставляет каждый из трёх инстансов"
  pc_finish --max-dups 0 --p99-max 3
}

s_kill_one() {
  pc_begin kill-one
  trio "$MIX"
  tfault 'delayMs=30' # a call on the wire at any moment of the kill
  local n i kills=3
  for n in $(seq 1 "$kills"); do
    sleep 10
    i=$((1001 + n % 3))
    pc_kill "$i"
    sleep 5
    pc_shop "$i"
  done
  sleep 5
  pc_finish --min-dups 1 --max-dups $((kills * BATCH_DUPS))
}

s_kill_all_but_one() {
  pc_begin kill-all-but-one
  trio "$MIX"
  tfault 'delayMs=30'
  sleep 15
  pc_kill 1002
  pc_kill 1003
  check "$(passes effects_grow 10)" "выживший доставляет один за всех, пока двух других нет"
  check "$(passes is "$(stats 1001 | j 'len(d["failedByClass"])')" = 0)" "писатели выжившего не падали"
  pc_shop 1002
  pc_shop 1003
  sleep 10
  pc_finish --min-dups 1 --max-dups $((2 * BATCH_DUPS))
}

s_kill_all() {
  pc_begin kill-all
  trio "$MIX"
  tfault 'delayMs=30'
  sleep 15
  pc_kill 1001
  pc_kill 1002
  pc_kill 1003
  sleep 5
  pc_shop 1001
  pc_shop 1002
  pc_shop 1003
  sleep 15
  pc_finish --min-dups 1 --max-dups $((3 * BATCH_DUPS))
}

s_rolling_restart() {
  pc_begin rolling-restart
  trio "$MIX"
  sleep 10
  local i took ok=0 bound=$((15 + 8))
  for i in 1001 1002 1003; do
    took=$(pc_term "$i")
    log "stop of $i took $took s"
    [ "$took" -le "$bound" ] || ok=1
    [ "$(st $((1001 + (i - 1000) % 3)) 'sum(1 for x in d["instances"] if x["live"])')" = 2 ] || ok=1
    fault "SIGTERM экземпляра $i: остановка $took с"
    pc_shop "$i"
    sleep 8
  done
  check "$ok" "каждая остановка не дольше shutdownTimeout + 8 с, строка инстанса удалена сразу"
  pc_finish --max-dups 0
}

s_restart_past_deadline() { # the batch outlives shutdownTimeout: interrupted, redelivered, nothing lost
  pc_begin restart-past-deadline
  trio "$MIX" -Doutbox.relay.shutdownTimeout=1s
  tfault 'delayMs=150'
  sleep 10
  local i took ok=0
  for i in 1001 1002 1003; do
    took=$(pc_term "$i")
    [ "$took" -le $((1 + 8)) ] || ok=1
    fault "SIGTERM экземпляра $i за $took с: батч не успел"
    pc_shop "$i" -Doutbox.relay.shutdownTimeout=1s
    sleep 5
  done
  check "$ok" "каждая остановка не дольше shutdownTimeout + 8 с при идущем батче"
  tfault 'delayMs=0'
  pc_finish
}

s_concurrent_start() { # three instances bootstrap one empty database at once
  pc_begin concurrent-start
  pc_target
  local i
  for i in 1001 1002 1003; do
    env HTTP_PORT=$((8079 + i)) METRICS_PORT=$((8084 + i)) POSTGRES_URL="$PC_PG_URL" TARGET_DB_URL="$PC_TARGET_DB_URL" \
      TARGET_URL="$PC_TARGET_URL" INSTANCE_NAME="app-$i" "JAVA_OPTS=-Xmx768m" "$SHOP" >> "$LOGS/pc-$SCN-$i.log" 2>&1 &
    APP_PIDS[i]=$!
  done
  local ok=0
  for i in 1001 1002 1003; do
    wait_for 90 curl -sf "localhost:$((8084 + i))/system/readiness" || ok=1
    pc_load "$i" "$MIX"
  done
  check "$ok" "все три стартовали на пустой базе одновременно"
  sleep 15
  pc_finish --max-dups 0
}

s_freeze_instance() { # SIGSTOP as a long GC pause or a frozen VM: its shards free up by idle_in_transaction_session_timeout; on SIGCONT it just goes on
  pc_begin freeze-instance
  trio "$MIX" -Doutbox.relay.batchTimeout=5s
  tfault 'delayMs=30'
  sleep 10
  local pid=${APP_PIDS[1003]}
  kill -STOP "$pid"
  fault "SIGSTOP экземпляра 1003 на 50 с"
  check "$(passes effects_grow 10)" "двое других доставляют"
  sleep 40
  kill -CONT "$pid"
  fault "SIGCONT"
  sleep 15
  check "$(passes ours "$pid")" "оттаявший процесс жив"
  check "$(passes gt "$(on 1003 metric outbox_messages_delivered_total)" 0)" "и доставлял"
  pc_finish
}
