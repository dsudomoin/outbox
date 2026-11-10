# Faults that meet the adapter, not the core: a fleet of Kora and Spring Boot instances on one outbox, requests that
# send while the instance stops, a start without its database. SHOP_BIN picks the app in shop's place (the store, say);
# the mixed fleet runs both.

# A team moving its service from Kora to Spring Boot: both on the same tables at once under load, kill -9 of either,
# the fleet replaced instance by instance, one of them back to Kora.
s_mixed_fleet() {
  pc_begin mixed-fleet
  local kora=$PC/shop/build/install/shop-n/bin/shop spring=${STORE:-$STORE41} i ok=0 took
  pc_target
  pc_app "$kora" 1001
  pc_app "$spring" 1002
  pc_app "$kora" 1003
  tfault 'delayMs=30'
  for i in 1001 1002 1003; do pc_load "$i" "$MIX"; done
  sleep 12
  for i in 1001 1002 1003; do [ "$(on "$i" metric outbox_messages_delivered_total)" -gt 0 ] || ok=1; done
  check "$ok" "Kora и Spring Boot на одних таблицах: доставляет каждый из трёх"
  pc_kill 1002
  sleep 5
  pc_app "$spring" 1002
  sleep 5
  pc_kill 1003
  sleep 5
  pc_app "$kora" 1003
  sleep 5
  ok=0
  for i in 1001 1003; do
    took=$(pc_term "$i")
    [ "$took" -le $((15 + 8)) ] || ok=1
    fault "SIGTERM Kora-инстанса $i за $took с, на его место Spring Boot"
    pc_app "$spring" "$i"
    sleep 6
  done
  took=$(pc_term 1002)
  [ "$took" -le $((15 + 8)) ] || ok=1
  fault "SIGTERM Spring-инстанса 1002 за $took с, на его место снова Kora"
  pc_app "$kora" 1002
  sleep 8
  check "$ok" "каждая остановка, Kora и Spring Boot, не дольше shutdownTimeout + 8 с"
  ok=0
  [ "$(st 1001 'sum(1 for x in d["instances"] if x["live"])')" = 3 ] || ok=1
  check "$ok" "в /status три живых инстанса, строки остановленных удалены"
  pc_finish --min-dups 1 --max-dups $((2 * BATCH_DUPS))
}

# HTTP requests that send keep coming while the instance stops: a request answered 200 has committed its message, and
# it is delivered; one the stop turns away is refused, never left half done; the stop stays bounded.
s_sigterm_inflight() {
  pc_begin sigterm-inflight
  trio "$MIX"
  sleep 8
  local answers=$RESULT_DIR/$SCN.answers stop=$RESULT_DIR/$SCN.stop took
  rm -f "$answers" "$stop"
  (
    n=0
    while [ ! -f "$stop" ]; do
      n=$((n + 1))
      s=$(date +%s)
      code=$(curl -s -o "$answers.body" -w '%{http_code}' --max-time 20 -XPOST \
        "$(pub 1003)/tx/send?topic=payments&key=inflight-$((n % 7))" || true)
      echo "$s $(date +%s) $code $(cat "$answers.body" 2> /dev/null)" >> "$answers"
      rm -f "$answers.body"
    done
  ) &
  local client=$!
  sleep 3
  took=$(pc_term 1003)
  fault "SIGTERM экземпляра 1003 за $took с, пока к нему идут запросы с отправкой"
  sleep 2
  touch "$stop"
  wait "$client" 2> /dev/null || true
  check "$(passes is "$took" -le $((15 + 8)))" "остановка под запросами не дольше shutdownTimeout + 8 с: $took с"
  local ok200 hung missing
  ok200=$(awk '$3 == 200' "$answers" | wc -l | tr -d ' ')
  hung=$(awk '$2 - $1 >= 19' "$answers" | wc -l | tr -d ' ')
  check "$(passes gt "$ok200" 10)" "запросы шли до остановки и во время неё: $ok200 ответов 200"
  local ids found
  ids=$(awk '$3 == 200' "$answers" | python3 -c '
import json, sys
print(",".join("'"'"'" + json.loads(l.split(" ", 3)[3])["id"] + "'"'"'" for l in sys.stdin if l.strip()))')
  found=$(psql_q "select count(*) from pc_sent where message_id in (${ids:-null})")
  missing=$((ok200 - found))
  check "$(passes is "$missing" = 0)" "у каждого ответа 200 закоммичено сообщение: не найдено $missing"
  check "$(passes is "$hung" = 0)" "ни один запрос не завис дольше 20 с"
  log "answers by status: $(awk '{print $3}' "$answers" | sort | uniq -c | tr '\n' ' ')"
  pc_shop 1003
  sleep 5
  pc_finish --max-dups 0
}

# The database is down when an instance starts, then it pauses mid-start and the instance gets SIGTERM: neither start
# comes up ready nor hangs; once the database is back an instance starts and the fleet delivers everything.
s_start_db_down() {
  pc_begin start-db-down
  pc_target
  pc_shop 1001
  pc_load 1001 "$MIX"
  sleep 8
  docker stop -t 5 "$PG" > /dev/null
  fault "Postgres остановлен, старт инстанса 1002"
  local log=$LOGS/pc-$SCN-1002.log pid t0 code=running ready=0
  t0=$(now)
  env HTTP_PORT=$((8079 + 1002)) METRICS_PORT=$((8084 + 1002)) POSTGRES_URL="$PC_PG_URL" TARGET_DB_URL="$PC_TARGET_DB_URL" \
    TARGET_URL="$PC_TARGET_URL" INSTANCE_NAME=app-1002 "JAVA_OPTS=-Xmx768m" "$SHOP" >> "$log" 2>&1 &
  pid=$!
  while ours "$pid" && [ $(($(now) - t0)) -lt 90 ]; do
    curl -sf "localhost:$((8084 + 1002))/system/readiness" > /dev/null && ready=1
    sleep 1
  done
  if ours "$pid"; then kill -9 "$pid"; wait "$pid" 2> /dev/null || true; else code=0; wait "$pid" || code=$?; fi
  local took=$(($(now) - t0))
  log "start without the database: exit $code after $took s, ready=$ready"
  check "$(passes is "$ready" = 0)" "без базы инстанс ни разу не стал ready"
  check "$(passes is "$code" != running)" "старт без базы завершился сам, код $code, за $took с"
  docker start "$PG" > /dev/null
  wait_for 60 docker exec "$PG" pg_isready -U outbox -d outbox -q
  sleep 3

  docker pause "$PG" > /dev/null
  fault "Postgres заморожен, старт инстанса 1003 и SIGTERM через 5 с"
  log=$LOGS/pc-$SCN-1003.log
  env HTTP_PORT=$((8079 + 1003)) METRICS_PORT=$((8084 + 1003)) POSTGRES_URL="$PC_PG_URL" TARGET_DB_URL="$PC_TARGET_DB_URL" \
    TARGET_URL="$PC_TARGET_URL" INSTANCE_NAME=app-1003 "JAVA_OPTS=-Xmx768m" "$SHOP" >> "$log" 2>&1 &
  pid=$!
  sleep 5
  t0=$(now)
  kill "$pid" 2> /dev/null || true
  while ours "$pid" && [ $(($(now) - t0)) -lt 90 ]; do sleep 1; done
  if ours "$pid"; then code=running; kill -9 "$pid"; wait "$pid" 2> /dev/null || true; else code=0; wait "$pid" || code=$?; fi
  took=$(($(now) - t0))
  docker unpause "$PG" > /dev/null
  log "SIGTERM during a start on a frozen database: exit $code after $took s"
  check "$(passes is "$code" != running)" "SIGTERM посреди старта на замёрзшей базе: процесс вышел за $took с"
  check "$(passes is "$took" -le 45)" "выход после SIGTERM посреди старта не дольше 45 с: $took с"

  sleep 3
  pc_shop 1002
  pc_load 1002 "$MIX"
  sleep 12
  pc_finish
}
