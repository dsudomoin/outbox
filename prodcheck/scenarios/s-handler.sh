# Handlers: «Обработка сбоев», «Изоляция», «Дедлайны», «Отложить запись», «Ждать», «Независимые цели».
PAY='rate=100&writers=4&keys=30&topics=payments'
free_threads() { on "$1" metric outbox_handler_threads_free; }
no_free_threads() { [ "$(free_threads "$1")" = 0 ]; }
retry_rows() { [ "$(psql_q 'select count(*) from outbox_retry')" -gt 0 ]; }
parked_rows() { [ "$(psql_q 'select count(*) from outbox_parked')" -gt 0 ]; }
waits_on() { [ "$(st "$1" 'd["waitingSize"]')" -gt 0 ]; }

courier_pair() { # courier_pair LOAD [-Dkey=value...]: the target and two couriers
  local load=$1 i
  shift
  pc_target
  for i in 1001 1002; do pc_courier "$i" "$@"; done
  for i in 1001 1002; do pc_load "$i" "$load"; done
}

s_slow_handler() { # the call outlives 80 % of batchTimeout: wind-down, the rest NotAttempted, no record lost
  pc_begin slow-handler
  trio 'rate=30&writers=3&keys=30&topics=payments'
  tfault 'delayMs=120' # 100 records × 120 ms = 12 s, batchTimeout 10 s
  sleep 40
  check "$(passes logs_have 'wind|deadline|NotAttempted|batchTimeout|abandon' )" "сворачивание или отказ от вызова видны в логе"
  tfault 'delayMs=0'
  pc_finish
}

s_failing_key() { # one key fails: it waits in the retry table, the others flow, its order holds
  pc_begin failing-key
  trio "$PAY"
  sleep 5
  tfault 'failKeys=k7'
  fault "цель отвергает ключ k7 на 30 с"
  local from=$(now)
  sleep 30
  local to=$(now)
  check "$(passes retry_rows)" "ключ в retry-таблице"
  check "$(passes logs_have 'streams diverted')" "WARN «streams diverted to retry»"
  check "$(passes gt "$(on 1001 metric 'outbox_messages_failed_total')" 0)" "растёт outbox.messages.failed{outcome=retry}"
  tfault 'failKeys='
  # the refused key's attempts are arrivals too, so no bound on duplicates here
  pc_finish --slow-keys k7 --p99-max 3 --slow-from "$from" --slow-to "$to"
}

s_flaky() { # 30 % of the calls fail: retries, the order of every key holds
  pc_begin flaky
  trio "$PAY"
  tfault 'failPercent=30'
  sleep 30
  tfault 'failPercent=0'
  pc_finish
}

s_poison() { # a record refused for good goes to the dead table; its key and shard go on
  pc_begin poison
  trio 'rate=100&writers=4&keys=30&topics=payments,ledger,projection&tags=poison:40'
  sleep 25
  check "$(passes logs_have 'ERROR.*(dead|Dead)')" "ERROR на перенос в dead-таблицу"
  local n
  n=$(curl -sf "localhost:$((8084 + 1001))/metrics" | grep -E '^outbox_messages_failed_total.*outcome="dead"' | awk '{s += $2} END {printf "%.0f", s}')
  check "$(passes gt "${n:-0}" 0)" "outbox.messages.failed{outcome=dead}: $n"
  pc_finish --dead-tags poison --exempt-tags poison --slow-tags poison --p99-max 3 --max-dups 0
}

s_max_attempts() { # retries run out: dead with the attempts counted, the key goes on; a redrive repairs it
  pc_begin max-attempts
  trio "$PAY" -Doutbox.relay.retry.maxAttempts=3
  sleep 5
  tfault 'failKeys=k5'
  sleep 25
  tfault 'failKeys='
  local i
  for i in 1001 1002 1003; do pc_unload "$i"; done
  local dead attempts
  dead=$(psql_q "select count(*) from outbox_dead where convert_from(key, 'UTF8') = 'k5'")
  attempts=$(psql_q "select coalesce(min(attempts), 0) || '/' || coalesce(max(attempts), 0) from outbox_dead")
  check "$(passes gt "$dead" 0)" "$dead записей ключа в dead-таблице, попыток $attempts"
  no_retry() { [ "$(psql_q 'select count(*) from outbox_retry')" = 0 ]; }
  wait_for 30 no_retry || true # the letters still coming, then the call that sends them all back
  redrive_until_empty 1001 '{"filter":{"topic":"payments"}}' 'topic=payments'
  REPAIRED="POST dead-letters/redrive по фильтру после починки цели"
  pc_finish --dead-tags normal --exempt-topics payments
}

s_stuck_head_fix() { # maxAttempts = 0, the key fails for good: no code and no full stop: a rolling config change and the admin API
  pc_begin stuck-head-fix
  trio "$PAY"
  sleep 5
  tfault 'failKeys=k5'
  sleep 15
  check "$(passes retry_rows)" "ключ стоит в retry-таблице, остальные идут"
  check "$(passes effects_grow 5 payments)" "остальные ключи доставляются"
  local i
  for i in 1001 1002 1003; do
    inst_stop "$i"
    pc_shop "$i" -Doutbox.relay.retry.maxAttempts=2
    sleep 3
  done
  fault "rolling-рестарт с maxAttempts = 2"
  sleep 10
  for i in 1001 1002 1003; do pc_unload "$i"; done
  tfault 'failKeys='
  check "$(passes wait_for 60 dead_for_key k5)" "записи ключа ушли в dead-таблицу"
  redrive_until_empty 1001 '{"filter":{"topic":"payments"}}' 'topic=payments'
  REPAIRED="rolling-рестарт с maxAttempts, затем POST dead-letters/redrive"
  pc_finish --dead-tags normal --exempt-topics payments
}
dead_for_key() { [ "$(psql_q "select count(*) from outbox_dead where convert_from(key, 'UTF8') = '$1'")" -gt 0 ]; }
# redrive_until_empty I BODY COUNT_QUERY: the documented «call again» loop, until no letter of the filter is left
redrive_until_empty() {
  local i=$1 body=$2 q=$3 left=1 n=0
  until [ "$left" = 0 ] || [ "$n" -gt 30 ]; do
    admin "$i" POST dead-letters/redrive "$body" > /dev/null
    left=$(admin_body "$i" GET "dead-letters/count?$q" | j 'd["count"]')
    n=$((n + 1))
    sleep 1
  done
  check "$(passes is "$left" = 0)" "dead letters переотправлены (вызовов $n)"
}

s_hang_one_instance() { # a call that ignores interrupts takes the instance's handler threads; the others take its shards
  pc_begin hang-one-instance
  pc_target
  local i
  for i in 1001 1002; do pc_shop "$i"; pc_load "$i" 'rate=100&writers=3&keys=30&topics=ledger,payments'; done
  sleep 5
  hctl 1001 'route=ledger&mode=hang-hard'
  fault "обработчик ledger на экземпляре 1001 завис и не отвечает на прерывание"
  check "$(passes wait_for 120 no_free_threads 1001)" "outbox.handler.threads.free = 0 на зависшем инстансе"
  check "$(passes is "$(st 1002 'sum(1 for x in d["instances"] if x["live"])')" = 2)" "heartbeat зависшего жив: алерт только по threads.free"
  check "$(passes effects_grow 8 ledger)" "другой инстанс забрал шарды, цель ledger доставляется"
  local took
  took=$(pc_term 1001)
  check "$(passes is "$took" -le 30)" "зависший инстанс останавливается за ограниченное время: $took с"
  pc_shop 1001
  REPAIRED="рестарт одного зависшего инстанса"
  pc_finish
}

s_hang_everywhere() { # every instance's handler hung: delivery stands (documented), resumes when the calls return
  pc_begin hang-everywhere
  pc_target
  local i
  for i in 1001 1002; do pc_shop "$i"; pc_load "$i" 'rate=100&writers=3&keys=30&topics=ledger,payments'; hctl "$i" 'route=ledger&mode=hang-hard'; done
  check "$(passes wait_for 120 no_free_threads 1001)" "потоки обработчика заняты на 1001"
  check "$(passes wait_for 60 no_free_threads 1002)" "потоки обработчика заняты на 1002"
  for i in 1001 1002; do hctl "$i" 'route=ledger&mode=ok'; done
  fault "вызовы вернулись"
  check "$(passes effects_grow 15)" "доставка пошла сама после возврата вызовов"
  pc_finish
}

s_routes_isolation() { # one route failing: its records retry, the routes beside it deliver in the same calls
  pc_begin routes-isolation
  pc_target
  local i
  for i in 1001 1002 1003; do pc_shop "$i"; pc_load "$i" 'rate=100&writers=4&keys=30&topics=payments,ledger,projection'; hctl "$i" 'route=ledger&mode=fail'; done
  local from=$(now)
  check "$(passes effects_grow 15 payments)" "payments идёт, пока ledger сбоит"
  check "$(passes effects_grow 5 projection)" "projection идёт, пока ledger сбоит"
  local to=$(now)
  for i in 1001 1002 1003; do hctl "$i" 'route=ledger&mode=ok'; done
  # one failing route of three leaves the other two delivered; its own records wait
  pc_finish --slow-from "$from" --slow-to "$to"
}

s_bad_batch() { # handleBatch throws, throws Error or returns a broken list: all of the call retried, nothing lost
  pc_begin bad-batch
  courier_pair 'rate=100&writers=4&keys=30&topics=deliveries,receipts'
  sleep 5
  local mode i
  for mode in throw error null-list wrong-size null-element; do
    for i in 1001 1002; do hctl "$i" "mode=$mode"; done
    fault "handleBatch: $mode"
    sleep 6
    for i in 1001 1002; do hctl "$i" 'mode=ok'; done
    sleep 4
  done
  check "$(passes logs_have 'ERROR')" "исключения и некорректные списки пишутся в ERROR"
  pc_finish
}

s_requeue() { # requeue: the key goes on, the record returns, kill -9 in the middle loses nothing and copies nothing
  pc_begin requeue
  courier_pair 'rate=100&writers=4&keys=30&topics=deliveries,receipts&tags=requeue:15'
  check "$(passes wait_for 30 parked_rows)" "записи лежат в parked"
  sleep 8
  pc_kill 1002
  sleep 4
  pc_courier 1002
  sleep 10
  check "$(passes gt "$(on 1001 metric outbox_messages_requeued_total)" 0)" "растёт outbox.messages.requeued"
  pc_finish --exempt-tags requeue
}

s_requeue_limit() { # requeue past maxRequeues: dead with «requeue limit reached»
  pc_begin requeue-limit
  courier_pair 'rate=100&writers=4&keys=30&topics=deliveries&tags=requeue:15' -Doutbox.relay.requeue.maxRequeues=2
  sleep 25
  check "$(passes gt "$(psql_q "select count(*) from outbox_dead where error like '%requeue limit reached%'")" 0)" "dead с ошибкой requeue limit reached"
  pc_finish --dead-tags requeue --exempt-tags requeue
}

s_retry_attempts() { # Outcome.retry a few times, then delivered: attempts count, the key's order holds
  pc_begin retry-attempts
  courier_pair 'rate=100&writers=4&keys=30&topics=deliveries,receipts&tags=retry3:25'
  sleep 25
  pc_finish
}

s_wait_handler() { # Outcome.waitFor: no attempt spent, no pause, key order holds, the alert fires, delivered when it can be
  pc_begin wait-handler
  pc_target
  local i
  for i in 1001 1002; do pc_courier "$i" -Doutbox.relay.waiting.alertAfter=5s -Doutbox.relay.waiting.delay=200ms; hctl "$i" 'waiting=true'; done
  for i in 1001 1002; do pc_load "$i" 'rate=100&writers=4&keys=30&topics=deliveries&tags=wait:50'; done
  check "$(passes wait_for 30 waits_on 1001)" "waitingSize > 0"
  sleep 12
  local a
  a=$(psql_q "select coalesce(max(attempts), 0) from outbox_retry where waiting_since is not null")
  check "$(passes is "$a" = 0)" "ожидание не тратит попыток"
  check "$(passes logs_have 'wait for an instance')" "ERROR обслуживания после alertAfter"
  check "$(passes is "$(psql_q 'select count(*) from outbox_dead')" = 0)" "в dead letters ничего"
  for i in 1001 1002; do hctl "$i" 'waiting=false'; done
  pc_finish --max-dups 0
}

s_typed_garbage() { # a typed route gets a payload it cannot decode and a tombstone: dead at once, its key goes on
  pc_begin typed-garbage
  pc_target
  pc_shop 1001
  pc_shop 1002
  pc_load 1001 'rate=50&writers=2&keys=10&topics=orders'
  sleep 5
  local out
  out=$(tx_send 1002 'topic=orders&key=k1&tag=garbage&raw=garbage')
  check "$(passes grep -q '"id"' <<< "$out")" "payload, который не декодируется, записан: ${out:0:100}"
  out=$(tx_send 1002 'topic=orders&key=k2&tag=garbage&raw=null')
  check "$(passes grep -q '"id"' <<< "$out")" "tombstone записан: ${out:0:100}"
  sleep 8
  check "$(passes is "$(psql_q "select count(*) from outbox_dead")" = 2)" "оба в dead-таблице"
  pc_finish --dead-tags garbage --exempt-tags garbage --max-dups 0
}
