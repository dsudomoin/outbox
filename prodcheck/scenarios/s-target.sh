# The target's outage: «Недоступная цель приостанавливает доставку и ничего не теряет», «Обработчик перед продом».
writers_clean() { # no writer failed so far on the given instances
  local i n=0
  for i in "$@"; do n=$((n + $(stats "$i" | j 'len(d["failedByClass"])'))); done
  [ "$n" = 0 ]
}

s_target_down() { # kill -9 of the target under load: writes and the other routes go on, HTTP records wait, none dead
  pc_begin target-down
  trio "$MIX"
  sleep 8
  inst_kill9 "$T"
  fault "kill -9 цели на 30 с"
  check "$(passes effects_grow 10 projection)" "другие маршруты идут, пока цель лежит"
  sleep 20
  check "$(passes writers_clean 1001 1002 1003)" "писатели не падали"
  check "$(passes gt "$(on 1001 metric outbox_shard_paused_total)" 0)" "растёт outbox.shard.paused"
  check "$(passes is "$(psql_q 'select count(*) from outbox_dead')" = 0)" "в dead letters ничего"
  pc_target
  pc_finish
}

s_target_long_outage() { # a long outage: retries back off to maxDelay and come back on their own
  pc_begin target-long-outage
  trio "$MIX" -Doutbox.relay.retry.maxDelay=20s
  sleep 5
  inst_kill9 "$T"
  fault "kill -9 цели на 90 с"
  sleep 90
  check "$(passes is "$(psql_q 'select count(*) from outbox_dead')" = 0)" "в dead letters ничего"
  pc_target
  local t=$(now) before
  before=$(target_q "select count(*) from pc_effect where sink = 'payments'")
  check "$(passes wait_for 40 sh -c "[ \"\$(docker exec $TGT psql -U outbox -d outbox -Atq -c \"select count(*) from pc_effect where sink = 'payments'\")\" -gt $before ]")" "доставка возобновилась через $(($(now) - t)) с после возврата цели (maxDelay 20 с)"
  pc_finish
}

s_target_db_down() { # the DB target's database goes away and returns
  pc_begin target-db-down
  trio "$MIX"
  sleep 8
  docker stop -t 2 "$TGT" > /dev/null
  fault "база цели остановлена на 25 с"
  source_grows() { local a b; a=$(psql_q "select count(*) from pc_effect"); sleep 10; b=$(psql_q "select count(*) from pc_effect"); [ "$b" -gt "$a" ]; }
  check "$(passes source_grows)" "маршрут на базе-источнике идёт"
  sleep 15
  check "$(passes writers_clean 1001 1002 1003)" "писатели не падали"
  docker start "$TGT" > /dev/null
  pc_finish
}

s_lost_response() { # the effect commits, the answer is lost: a duplicate arrival, one effect
  pc_begin lost-response
  trio "$PAY"
  tfault 'dropPercent=10'
  sleep 25
  tfault 'dropPercent=0'
  pc_finish --min-dups 1
}

s_late_request() { # the first request outlives the client's timeout and lands after its retry
  pc_begin late-request
  trio "$PAY"
  sleep 3
  tfault 'delayMs=3000' # the client gives up at 2 s, the target carries on
  fault "цель отвечает за 3 с при таймауте клиента 2 с"
  sleep 15
  tfault 'delayMs=0'
  pc_finish --min-dups 1
}

s_target_net() { # the connection to the target resets and goes silent
  pc_begin target-net
  proxy_up 55441 9090
  PC_TARGET_URL=http://localhost:55441
  trio "$PAY"
  local n
  for n in 1 2 3; do
    sleep 5
    proxy reset
    sleep 1
    proxy pass
  done
  proxy blackhole
  sleep 10
  proxy pass
  sleep 8
  pc_finish
}
