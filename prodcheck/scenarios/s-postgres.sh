# Postgres faults: «рестарты Postgres», connection loss, «Таймауты и зависания», «Репликация, failover и PITR».
progress() { # after a database fault every instance is the same process and commits again
  local i ok=0 a b
  for i in "$@"; do
    a=$(stats "$i" | j 'd["committed"]') || a=-1
    sleep 6
    b=$(stats "$i" | j 'd["committed"]') || b=-1
    ours "${APP_PIDS[$i]}" && [ "$b" -gt "$a" ] || ok=1
  done
  check "$ok" "те же процессы снова коммитят: рестарта приложений не понадобилось"
}

s_pg_restart() {
  pc_begin pg-restart
  trio "$MIX"
  sleep 12
  fault "docker restart (fast shutdown)"
  docker restart -t 10 "$PG" > /dev/null
  sleep 15
  progress 1001 1002 1003
  fault "pg_ctl stop -m immediate и старт (падение сервера)"
  docker exec -u postgres "$PG" pg_ctl -D /var/lib/postgresql/data stop -m immediate > /dev/null 2>&1 || true
  sleep 3
  docker start "$PG" > /dev/null
  sleep 15
  progress 1001 1002 1003
  pc_finish
}

s_pg_pause() {
  pc_begin pg-pause
  trio "$MIX" -Doutbox.relay.shutdownTimeout=5s
  sleep 12
  fault "docker pause на 20 с"
  docker pause "$PG" > /dev/null
  sleep 5
  local took
  took=$(pc_term 1003)
  check "$(passes is "$took" -le $((5 + 8 + 5)))" "остановка при замёрзшей базе ограничена: $took с"
  sleep 12
  docker unpause "$PG" > /dev/null
  sleep 15
  progress 1001 1002
  pc_shop 1003 -Doutbox.relay.shutdownTimeout=5s
  sleep 5
  pc_finish
}

s_pg_conn_kill() { # every backend of the apps killed, again and again
  pc_begin pg-conn-kill
  trio "$MIX"
  sleep 8
  local n
  for n in 1 2 3 4 5; do
    fault "pg_terminate_backend всех соединений приложений"
    psql_q "select count(pg_terminate_backend(pid)) from pg_stat_activity where application_name like 'app-%'" > /dev/null
    sleep 6
  done
  progress 1001 1002 1003
  pc_finish
}

s_pg_blackhole() { # the network goes silent for 25 s, then comes back; socketTimeout bounds every wait
  pc_begin pg-blackhole
  proxy_up 55440 55432
  PC_PG_URL=jdbc:postgresql://localhost:55440/outbox
  trio "$MIX" -Ddb.dsProperties.socketTimeout=5 -Doutbox.relay.shutdownTimeout=5s
  sleep 10
  proxy blackhole
  sleep 8
  local took
  took=$(pc_term 1003)
  check "$(passes is "$took" -le $((5 + 8 + 5)))" "остановка при молчащей сети ограничена: $took с"
  sleep 17
  proxy pass
  sleep 15
  check "$(passes gt "$(on 1001 metric outbox_relay_failures_total)" 0)" "растёт outbox.relay.failures"
  progress 1001 1002
  pc_shop 1003 -Ddb.dsProperties.socketTimeout=5 -Doutbox.relay.shutdownTimeout=5s
  sleep 3
  pc_finish
}

s_pg_silent_drop() { # a firewall forgets the connections: open sockets are dead for good, new ones work. Self-heal with socketTimeout
  pc_begin pg-silent-drop
  proxy_up 55440 55432
  PC_PG_URL=jdbc:postgresql://localhost:55440/outbox
  trio "$MIX" -Ddb.dsProperties.socketTimeout=5
  sleep 10
  proxy drop
  sleep 25
  progress 1001 1002 1003
  # a dead session may still hold maintenance's session lock on the server: maintenance must come back all the same
  maintained() { [ "$(st 1001 'int(d.get("maintenanceSinceSuccessSeconds") or 999)')" -lt 20 ]; }
  check "$(passes wait_for 120 maintained)" "обслуживание снова проходит: since_success < 20 с"
  pc_finish
}

s_pg_silent_drop_no_timeout() { # the same with pgjdbc's default socketTimeout = 0: the relay heals by outbox.networkTimeout
  pc_begin pg-silent-drop-no-timeout
  proxy_up 55440 55432
  PC_PG_URL=jdbc:postgresql://localhost:55440/outbox
  local opts='-Ddb.dsProperties.socketTimeout=0 -Doutbox.networkTimeout=10s'
  trio "$MIX" $opts
  sleep 10
  proxy drop
  sleep 3
  tx_send 1001 'topic=payments&key=probe&tag=normal' > /dev/null
  delivered() { [ "$(target_q "select count(*) from pc_effect where key = 'probe'")" = 1 ]; }
  check "$(passes wait_for 60 delivered)" "запись после обрыва доставлена без рестарта: воркеры вышли по networkTimeout"
  # the writers' own transactions hang on their sockets, that is the application's socketTimeout: their threads go with a restart
  local i
  for i in 1001 1002 1003; do
    LOADS[i]=""
    inst_stop "$i"
    pc_shop "$i" $opts
  done
  REPAIRED="relay сама; потоки писателей приложения висят без его socketTimeout, ушли с рестартом"
  pc_finish --exempt-topics none
}

s_pg_conn_exhausted() { # another client takes every connection slot for 20 s
  pc_begin pg-conn-exhausted
  trio "$MIX"
  sleep 10
  fault "чужой клиент занимает все слоты max_connections на 20 с"
  docker exec -d "$PG" sh -c 'for i in $(seq 1 300); do psql -U outbox -d postgres -Atq -c "select pg_sleep(20)" > /dev/null 2>&1 & done; wait'
  sleep 25
  progress 1001 1002 1003
  pc_finish
}

s_restore_point() { # a physical restore to an earlier point, the outcome of PITR or of a failover to a lagging replica
  pc_begin restore-point
  trio "$MIX"
  sleep 15
  local volume=kora-outbox-prodcheck-backup
  fault "холодная физическая копия базы"
  docker stop -t 10 "$PG" > /dev/null
  docker volume rm "$volume" > /dev/null 2>&1 || true
  docker run --rm --volumes-from "$PG" -v "$volume:/backup" postgres:16 sh -c 'cp -a /var/lib/postgresql/data/. /backup/'
  docker start "$PG" > /dev/null
  sleep 20
  fault "база возвращена к этой копии: коммиты после неё пропали, инстансы работают"
  docker stop -t 10 "$PG" > /dev/null
  source_replace "-v $volume:/var/lib/postgresql/data"
  sleep 25
  progress 1001 1002 1003
  pc_finish --allow-phantoms --min-dups 1
}
