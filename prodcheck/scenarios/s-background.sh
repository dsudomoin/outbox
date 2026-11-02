# The threads beside the relay and OutboxRelay.drain(): «Алерты» (route_unchecked), «Порядок запуска и остановки»,
# «Таймауты и зависания».
route_unchecked() { on "$1" metric outbox_messages_route_unchecked_total; }
silent_url() { proxy_up 55440 55432; PC_PG_URL=jdbc:postgresql://localhost:55440/outbox; }

s_route_unchecked() { # the heartbeat cannot read the others' reports past 3 × maintenanceInterval: sends go through unchecked, counted, and it heals by itself
  pc_begin route-unchecked
  trio "$PAY" -Doutbox.partitions.maintenanceInterval=2s
  sleep 5
  check "$(passes is "$(route_unchecked 1001)" = 0)" "пока heartbeat читает отчёты, счётчик 0"
  docker exec -d "$PG" psql -U outbox -d outbox -Atq -c "begin; lock table outbox_instance in access exclusive mode; select pg_sleep(30); commit"
  fault "таблицу отчётов инстансов держит чужая блокировка 30 с: heartbeat не читает, 3 × maintenanceInterval = 6 с"
  counted() { [ "$(route_unchecked 1001)" -gt 0 ]; }
  local t=$(now)
  check "$(passes wait_for 25 counted)" "outbox.messages.route_unchecked растёт: через $(($(now) - t)) с после начала, $(route_unchecked 1001)"
  check "$(passes logs_have 'outbox heartbeat of instance .* failed')" "WARN из строки алерта: $(cat "$LOGS/pc-$SCN-"100*.log | grep -m1 'heartbeat of instance' | cut -d' ' -f3- | cut -c1-140)"
  check "$(passes logs_have 'through unchecked')" "WARN: $(cat "$LOGS/pc-$SCN-"100*.log | grep -m1 'through unchecked' | cut -d' ' -f3- | cut -c1-180)"
  local failed
  failed=$(stats 1001 | j 'sum(d["failedByClass"].values())')
  check "$(passes is "$failed" = 0)" "ни одна отправка не отвергнута: последнее чтение их допускало, ничего не теряется"
  sleep $((t + 34 - $(now)))
  local a b
  a=$(route_unchecked 1001)
  sleep 10
  b=$(route_unchecked 1001)
  check "$(passes is "$a" = "$b")" "блокировка ушла: счётчик больше не растёт без рестарта ($a, через 10 с $b)"
  check "$(passes is "$(psql_q "select count(*) from outbox_instance where expires_at > now()")" = 3)" "все три строки снова живы"
  pc_finish
}

s_drain() { # relay off and on, parked and waiting records, the hook's run at the end, a halt, a target that is down
  pc_begin drain
  BASES=(outbox courier_main)
  pc_target
  local off='-Doutbox.relay.enabled=false -Doutbox.table=courier_main -Doutbox.inbox.table=courier_inbox' out
  pc_courier 1001 $off
  hctl 1001 'waiting=true'
  pc_load 1001 'rate=200&writers=2&keys=20&topics=deliveries,receipts&tags=requeue:25,wait:40,poison:47'
  sleep 8
  pc_unload 1001
  check "$(passes is "$(effects)" = 0)" "без relay ничего не доставлено"
  out=$(drain_call 1001 120)
  log "drain 1: $out"
  check "$(passes grep -q delivered <<< "$out")" "drain с выключенным relay вернул число: $out"
  check "$(passes gt "$(effects)" 0)" "и доставил: $(effects) эффектов"
  local status
  status=$(admin_body 1001 GET status)
  log "after drain: $(echo "$status" | j '{k: d[k] for k in ("waitingSize", "parkedSize", "retrySize")}')"
  check "$(passes gt "$(echo "$status" | j 'd["waitingSize"]')" 0)" "drain вернулся, пока записи ждут: waitingSize $(echo "$status" | j 'd["waitingSize"]')"
  # a halt in the table stops drain, as the README says: maintenance does not run with the relay off
  psql_q "insert into courier_main_halt (max_tick_xmax) values ('1')" > /dev/null
  out=$(drain_call 1001 30)
  check "$(passes grep -q IllegalStateException <<< "$out")" "при записанной остановке drain бросает: $(echo "$out" | cut -c1-150)"
  psql_q "delete from courier_main_halt" > /dev/null
  # a target that is down: no progress for 2 × (batchTimeout + 5 s) is an exception, not a hang
  tfault 'mode=error'
  tx_send 1001 'topic=deliveries&key=stall&tag=normal' > /dev/null
  local t=$(now)
  out=$(drain_call 1001 90)
  check "$(passes grep -q 'IllegalStateException\|delivered' <<< "$out")" "цель лежит: drain вернулся за $(($(now) - t)) с: $(echo "$out" | cut -c1-170)"
  tfault 'mode=ok'
  hctl 1001 'waiting=false'
  # parked and waiting records come out over several calls with nothing else running
  local n
  for n in 1 2 3 4 5 6 7 8 9 10; do
    out=$(drain_call 1001 60)
    log "drain again: $out, left: $(psql_q "select (select count(*) from courier_main_parked), (select count(*) from courier_main_retry)")"
    [ "$(psql_q "select (select count(*) from courier_main_parked) + (select count(*) from courier_main_retry)")" != 0 ] || break
    sleep 2
  done
  check "$(passes is "$(psql_q "select (select count(*) from courier_main_parked) + (select count(*) from courier_main_retry)")" = 0)" "повторные drain разобрали отложенные и ждущие записи: вызовов $n"
  # relay on: drain contends with the workers and still returns
  inst_stop 1001
  pc_courier 1001 -Doutbox.table=courier_main -Doutbox.inbox.table=courier_inbox
  pc_load 1001 'rate=200&writers=2&keys=20&topics=deliveries,receipts'
  sleep 3
  out=$(drain_call 1001 60)
  check "$(passes grep -q delivered <<< "$out")" "с работающим relay и под нагрузкой drain вернулся: $out"
  pc_unload 1001
  # relay off with a hook: the letters the call made are decided inside the call
  pc_shop 1002 -Doutbox.relay.enabled=false -Dprodcheck.hook=policy
  tx_send 1002 'topic=payments&key=h1&tag=hook-discard' > /dev/null
  tx_send 1002 'topic=payments&key=h2&tag=hook-keep' > /dev/null
  out=$(drain_call 1002 60)
  check "$(passes is "$(psql_q "select count(*) from outbox_dead where status = 'pending'")" = 0)" "хук решил письма внутри того же вызова: $out"
  check "$(passes is "$(psql_q "select count(*) from outbox_dead where key = convert_to('h1', 'UTF8')")" = 0)" "discard хука удалил письмо"
  check "$(passes is "$(psql_q "select status from outbox_dead where key = convert_to('h2', 'UTF8')")" = kept)" "keep оставил письмо"
  inst_stop 1002
  pc_shop 1002 -Dprodcheck.hook=policy
  pc_finish --dead-tags poison,hook-keep --discard-tags hook-discard --exempt-tags requeue,wait,poison,hook-keep,hook-discard --timeout 120
}

# Silent-drop probes keep the server's locks in mind: a session of the dropped socket stays alive on the server.
hung_drain() { # hung_drain I LABEL BOUND: /drain must return within BOUND, or the stacks say where it hangs
  local i=$1 label=$2 bound=$3 t out rc=0
  t=$(now)
  out=$(curl -s --max-time "$bound" -XPOST "$(pub "$i")/drain") || rc=$?
  local took=$(($(now) - t))
  log "drain ($label): rc $rc after $took s: $(echo "$out" | cut -c1-200)"
  [ -n "$out" ] || thread_dump "$i" "drain-$label"
  check "$(passes is -n "$out")" "drain при молчащем сокете ($label) вернулся за $took с из $bound: $(echo "${out:-завис; стек в build/results/$SCN-$i-drain-$label.threads}" | cut -c1-170)"
}

s_drain_silent_drop() { # socketTimeout = 0: a drop under each part of drain. relay.networkTimeout bounds the shards' connection; and the rest?
  pc_begin drain-silent-drop
  pc_target
  silent_url
  local opts='-Ddb.dsProperties.socketTimeout=0 -Doutbox.relay.networkTimeout=10s -Doutbox.relay.enabled=false -Dprodcheck.hook=policy -Doutbox.deadLetters.timeout=3s'
  local bound=60 i=1001
  tfault 'delayMs=30'
  # 1. in a step, on the connection drain binds itself
  pc_shop "$i" $opts
  pc_load "$i" 'rate=300&writers=2&keys=30&topics=payments'
  sleep 6
  pc_unload "$i"
  ( sleep 2; echo drop > "$PROXY_CTL" ) &
  hung_drain "$i" step "$bound"
  fault "сеть: drop во время шага drain"
  wait $! 2> /dev/null || true
  echo pass > "$PROXY_CTL"
  tfault 'delayMs=0'
  inst_kill9 "$i"
  # 2. at the start, checkHalt: its statement goes into a silent network, which then forgets the connection
  pc_shop "$i" $opts
  sleep 2
  echo blackhole > "$PROXY_CTL"
  ( sleep 3; echo drop > "$PROXY_CTL" ) &
  hung_drain "$i" check-halt "$bound"
  fault "сеть: молчит с первого запроса drain (checkHalt), затем drop"
  wait $! 2> /dev/null || true
  echo pass > "$PROXY_CTL"
  inst_kill9 "$i"
  # 3. in the dead-letter run at the end: the hook hangs past its timeout, the rollback goes to a dead socket
  pc_shop "$i" $opts
  tx_send "$i" 'topic=payments&key=dl&tag=hook-keep' > /dev/null
  curl -sf -XPUT "$(pub "$i")/hook?mode=hang" > /dev/null
  ( sleep 2; echo drop > "$PROXY_CTL" ) &
  hung_drain "$i" dead-letters "$bound"
  fault "сеть: drop, пока проход dead letters держит письмо"
  wait $! 2> /dev/null || true
  echo pass > "$PROXY_CTL"
  curl -sf -XPUT "$(pub "$i")/hook?mode=ok" > /dev/null || true
  inst_kill9 "$i"
  # afterwards: relay on, plain URL
  PC_PG_URL=jdbc:postgresql://localhost:55432/outbox
  pc_shop 1002 -Dprodcheck.hook=policy
  REPAIRED="рестарт, если drain завис"
  pc_finish --dead-tags hook-keep --exempt-tags hook-keep --timeout 120
}

s_silent_drop_background() { # socketTimeout = 0: maintenance, the dead-letter pass and the heartbeat each lose their connection in the middle of a statement
  pc_begin silent-drop-background
  pc_target
  silent_url
  local opts='-Ddb.dsProperties.socketTimeout=0 -Doutbox.relay.networkTimeout=10s -Dprodcheck.hook=policy -Doutbox.deadLetters.timeout=30s -Doutbox.partitions.maintenanceInterval=2s -Doutbox.partitions.xidsPerPartition=1000 -Doutbox.partitions.premake=4'
  local i=1001
  pc_shop "$i" $opts
  sleep 3
  # the dead-letter pass: inside the hook, on a hanging call
  curl -sf -XPUT "$(pub "$i")/hook?mode=hang" > /dev/null
  tx_send "$i" 'topic=payments&key=dl&tag=hook-keep' > /dev/null
  # maintenance and the heartbeat: each waits on a row lock when the network drops it
  docker exec -d "$PG" psql -U outbox -d outbox -Atq -c "begin; select 1 from outbox_maintenance for update; select 1 from outbox_instance for update; select pg_sleep(12); commit"
  blocked() { [ "$(psql_q "select count(*) from pg_stat_activity where application_name = 'app-$i' and wait_event_type = 'Lock' and (query like '%_maintenance%' or query like '%_instance%')")" -ge 2 ]; }
  wait_for 8 blocked || true
  log "waiting on locks: $(psql_q "select left(query, 60) from pg_stat_activity where application_name = 'app-$i' and wait_event_type = 'Lock'" | tr '\n' ';')"
  # first silence, so that whatever is in flight stays in flight, the heartbeat's 100 ms lock wait too; then forgotten for good
  proxy blackhole
  sleep 5
  proxy drop
  sleep 8
  curl -sf -XPUT "$(pub "$i")/hook?mode=ok" > /dev/null
  echo pass > "$PROXY_CTL"
  local started=$(now)
  beat() { [ "$(psql_q "select count(*) from outbox_instance where expires_at > now() and last_seen > now() - interval '10 seconds'")" = 1 ]; }
  maintained() { [ "$(psql_q "select extract(epoch from clock_timestamp() - succeeded_at) < 10 from outbox_maintenance")" = t ]; }
  decided() { [ "$(psql_q "select count(*) from outbox_dead where status = 'pending'")" = 0 ]; }
  local limit=90 hb=-1 mt=-1 dl=-1 elapsed
  while elapsed=$(($(now) - started)); [ "$elapsed" -lt "$limit" ]; do
    if [ "$hb" = -1 ] && beat; then hb=$elapsed; fi
    if [ "$mt" = -1 ] && maintained; then mt=$elapsed; fi
    if [ "$dl" = -1 ] && decided; then dl=$elapsed; fi
    if [ "$hb" != -1 ] && [ "$mt" != -1 ] && [ "$dl" != -1 ]; then break; fi
    sleep 2
  done
  thread_dump "$i" after
  log "back after: heartbeat $hb, maintenance $mt, dead letters $dl (of $limit s)"
  check "$(passes is "$hb" != -1)" "heartbeat вернулся сам: строка инстанса свежая через $hb с (-1: нет за $limit с)"
  check "$(passes is "$mt" != -1)" "обслуживание вернулось само: since_success падает через $mt с"
  check "$(passes is "$dl" != -1)" "проход dead letters вернулся сам: письмо решено через $dl с"
  # partitions: xids go by, the premade stock must be made again
  local before after
  before=$(psql_q "select max(relname) from pg_class where relname ~ '^outbox_x[0-9]+$'")
  docker exec "$PG" psql -U outbox -d outbox -Atq -c "select pg_current_xact_id()" -c "do \$\$ begin for i in 1..3000 loop perform pg_current_xact_id(); commit; end loop; end \$\$" > /dev/null
  sleep 8
  local n
  n=$(psql_q "select count(*) from pg_class where relname ~ '^outbox_x[0-9]+$'")
  after=$(st "$i" 'd["premade"]')
  log "partitions $n, premade $after"
  check "$(passes is "$after" -ge 2)" "партиции продолжают создаваться: premade $after после 3000 xid при диапазоне 1000"
  # what it takes today when a check above failed: a restart
  pc_kill "$i"
  PC_PG_URL=jdbc:postgresql://localhost:55432/outbox
  pc_shop "$i" -Dprodcheck.hook=policy
  REPAIRED="рестарт, если поток завис"
  pc_finish --dead-tags hook-keep --exempt-tags hook-keep --timeout 60
}
