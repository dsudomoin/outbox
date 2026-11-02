# «Восстановление после регрессии xid»: a real logical migration to a new cluster under running instances.
halted() { [ "$(st 1001 'd["halted"]')" = True ]; }

migrate() { # writers stopped, backlog undelivered: pg_dump, a new cluster, pg_restore, the proxy switches. Instances keep running.
  local i dump=$LOGS/pc-$SCN-dump.bin
  for i in 1001 1002 1003; do pc_unload "$i"; done
  tfault 'mode=error'
  sleep 3
  local old
  old=$(psql_q 'select pg_current_xact_id()')
  fault "pg_dump, новый кластер, pg_restore; xid старого кластера $old"
  docker exec "$PG" pg_dump -Fc -U outbox outbox > "$dump"
  proxy refuse
  docker stop -t 5 "$PG" > /dev/null
  source_replace "" 55434
  docker exec "$PG" psql -U outbox -d postgres -Atq -c "drop database if exists outbox with (force)" -c "create database outbox"
  docker exec -i "$PG" pg_restore -U outbox -d outbox --no-owner < "$dump"
  local new
  new=$(psql_q 'select pg_current_xact_id()')
  check "$(passes gt "$old" "$new")" "счётчик xid нового кластера ниже: $old → $new"
  proxy pass 55434
  fault "соединения идут в новый кластер"
}

xid_begin() { # xid_begin NAME [-Dkey=value...]: three shops behind the proxy, a load that burned xids in the old cluster
  local name=$1
  shift
  pc_begin "$name"
  proxy_up 55440 55432
  PC_PG_URL=jdbc:postgresql://localhost:55440/outbox
  trio "$MIX" "$@"
  sleep 15
}

s_xid_auto() { # auto mode: the first maintenance run recovers, undelivered rows are delivered, delivered ones may repeat
  xid_begin xid-auto
  migrate
  sleep 2
  pc_kill 1003 # a crash in the middle of the cut-over changes nothing
  recovered() { logs_have 'xid regression detected.*recovered'; }
  check "$(passes wait_for 60 recovered)" "ERROR «xid regression detected ..., recovered: N rows re-sent»"
  tfault 'mode=ok'
  pc_shop 1003
  local i
  for i in 1001 1002 1003; do pc_load "$i" "$MIX"; done
  sleep 10
  pc_finish --timeout 200
}

s_xid_halt() { # halt mode: the relay stands until one admin call on a running instance
  xid_begin xid-halt -Doutbox.recovery.xidRegression=halt
  check "$(passes is "$(admin_code 1001 POST recovery/xid-regression)" = 409)" "до регрессии вызов отвечает 409"
  migrate
  check "$(passes wait_for 60 halted)" "/status: halted, relay стоит"
  local before=$(effects)
  sleep 5
  check "$(passes is "$(effects)" = "$before")" "пока остановка, ничего не доставляется"
  tfault 'mode=ok'
  local body
  body=$(admin 1001 POST recovery/xid-regression)
  check "$(passes is "$(echo "$body" | head -1)" = 200)" "POST recovery/xid-regression на живом инстансе: $(echo "$body" | tr '\n' ' ')"
  check "$(passes is "$(st 1002 'd["halted"]')" = False)" "остановка снята для всех"
  REPAIRED="POST recovery/xid-regression"
  local i
  for i in 1001 1002 1003; do pc_load "$i" "$MIX"; done
  sleep 10
  pc_finish --timeout 200
}

s_xid_max_rows() { # more rows than maxRows: relay halted, ERROR with the reason, one admin call
  xid_begin xid-max-rows -Doutbox.recovery.maxRows=0
  migrate
  check "$(passes wait_for 60 halted)" "/status: halted"
  stated() { logs_have 'relay halted'; }
  check "$(passes wait_for 30 stated)" "ERROR «relay halted» называет причину"
  tfault 'mode=ok'
  check "$(passes is "$(admin_code 1002 POST recovery/xid-regression)" = 200)" "POST recovery/xid-regression"
  REPAIRED="POST recovery/xid-regression"
  pc_finish --timeout 200
}
