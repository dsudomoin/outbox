# Several outboxes, apps and shard numbers: «Независимые цели», «Несколько инстансов», «Решардинг».
s_two_outboxes() { # the second outbox's handler hangs: the first flows; both lose nothing under kill -9
  pc_begin two-outboxes
  BASES=(outbox courier_audit)
  pc_target
  local i
  for i in 1001 1002; do
    pc_courier "$i"
    curl -sf -XPOST "$(pub "$i")/load?rate=80&writers=2&keys=20&topics=deliveries" > /dev/null
    curl -sf -XPOST "$(pub "$i")/load?rate=80&writers=2&keys=20&outbox=second" > /dev/null
    hctl "$i" 'route=second&mode=hang-hard'
  done
  fault "обработчик второго outbox завис на обоих инстансах"
  check "$(passes effects_grow 15 deliveries)" "первый outbox идёт"
  for i in 1001 1002; do hctl "$i" 'route=second&mode=ok'; done
  fault "второй снова отвечает"
  check "$(passes effects_grow 8 second)" "второй outbox пошёл сам"
  pc_kill 1002
  sleep 3
  pc_courier 1002
  sleep 5
  curl -sf -XDELETE "$(pub 1001)/load?outbox=second" > /dev/null || true
  pc_finish
}

s_two_apps_one_db() { # two different apps with their own tables on one database, a kill of each
  pc_begin two-apps-one-db
  BASES=(outbox courier_main)
  pc_target
  pc_shop 1001
  pc_shop 1002
  pc_courier 1003 -Doutbox.table=courier_main -Doutbox.inbox.table=courier_inbox
  pc_courier 1004 -Doutbox.table=courier_main -Doutbox.inbox.table=courier_inbox
  local i
  for i in 1001 1002; do pc_load "$i" "$MIX"; done
  for i in 1003 1004; do pc_load "$i" 'rate=100&writers=2&keys=20&topics=deliveries,receipts,routed'; done
  sleep 15
  pc_kill 1001
  pc_kill 1003
  sleep 5
  pc_shop 1001
  pc_courier 1003 -Doutbox.table=courier_main -Doutbox.inbox.table=courier_inbox
  sleep 10
  pc_finish
}

s_shards_mismatch() { # a new instance with another outbox.shards does not start while the others run; they do not care
  pc_begin shards-mismatch
  trio "$PAY"
  sleep 5
  local log=$LOGS/pc-$SCN-1004.log rc=0
  bounded 60 env HTTP_PORT=9083 METRICS_PORT=9088 POSTGRES_URL="$PC_PG_URL" TARGET_DB_URL="$PC_TARGET_DB_URL" TARGET_URL="$PC_TARGET_URL" \
    INSTANCE_NAME=app-1004 "JAVA_OPTS=-Xmx768m -Doutbox.shards=64" "$SHOP" > "$log" 2>&1 || rc=$?
  check "$(passes gt "$rc" 0)" "инстанс с другим числом шардов не стартует (код $rc)"
  check "$(passes grep -qE 'shards' "$log")" "отказ называет причину: $(grep -m1 -E 'shards' "$log" | cut -c1-160)"
  check "$(passes effects_grow 5)" "работающие инстансы этого не заметили"
  pc_finish --max-dups 0
}

s_reshard() { # planned reshard: delivered, every instance stopped, one starts with the new number. Documented limit, not a repair
  pc_begin reshard
  trio "$PAY"
  sleep 8
  local i
  for i in 1001 1002 1003; do pc_unload "$i"; done
  python3 "$PC/scenarios/ledger.py" --source "$PG" --target "$TGT" --base outbox --timeout 60 > /dev/null
  for i in 1001 1002 1003; do inst_stop "$i"; done
  fault "запланированный решардинг: все инстансы остановлены"
  sleep 70 # the newest tick must be older than a minute
  pc_shop 1001 -Doutbox.shards=64
  check "$(passes logs_have 'resharded .* from 256 to 64')" "INFO «outbox resharded»"
  pc_shop 1002
  pc_shop 1003
  for i in 1001 1002 1003; do pc_load "$i" "$PAY"; done
  sleep 10
  REPAIRED="не сбой: документированная процедура со всеми остановленными"
  pc_finish --max-dups 0
}

s_everything() { # all at once: writers with rollbacks, two apps, poison, requeue, flaky target, kills, a long transaction
  pc_begin everything
  BASES=(outbox courier_main)
  pc_target
  local i
  for i in 1001 1002; do pc_shop "$i"; pc_load "$i" 'rate=100&writers=4&keys=30&topics=payments,ledger,projection&rollbackPercent=5&tags=poison:60'; done
  for i in 1003 1004; do pc_courier "$i" -Doutbox.table=courier_main -Doutbox.inbox.table=courier_inbox; pc_load "$i" 'rate=100&writers=4&keys=30&topics=deliveries,receipts,routed&rollbackPercent=5&tags=requeue:40,retry3:50'; done
  tfault 'failPercent=10&dropPercent=5'
  curl -sf -XPOST "$(pub 1001)/tx/hold?ms=45000&topic=payments&key=held&tag=held" > /dev/null
  local n
  for n in 1 2 3 4 5 6; do
    sleep 10
    case $((n % 4)) in
      0) pc_kill 1001; sleep 3; pc_shop 1001 ;;
      1) pc_kill 1003; sleep 3; pc_courier 1003 -Doutbox.table=courier_main -Doutbox.inbox.table=courier_inbox ;;
      2) inst_kill9 "$T"; fault "kill -9 цели"; sleep 5; pc_target; tfault 'failPercent=10&dropPercent=5' ;;
      3) fault "pg_terminate_backend всех приложений"; psql_q "select count(pg_terminate_backend(pid)) from pg_stat_activity where application_name like 'app-%'" > /dev/null ;;
    esac
  done
  tfault 'failPercent=0&dropPercent=0'
  pc_finish --min-dups 1 --dead-tags poison --exempt-tags poison,requeue --timeout 300
}

s_reshard_stale() { # a frozen instance sleeps through a reshard: thawed, its relay stands and its writes are refused until a restart
  pc_begin reshard-stale
  PC_OPTS=-Doutbox.partitions.maintenanceInterval=2s trio "$PAY"
  sleep 8
  local i pid=${APP_PIDS[1003]}
  for i in 1001 1002 1003; do pc_unload "$i"; done
  python3 "$PC/scenarios/ledger.py" --source "$PG" --target "$TGT" --base outbox --timeout 60 > /dev/null
  kill -STOP "$pid"
  fault "экземпляр 1003 заморожен; остальные остановлены для решардинга"
  for i in 1001 1002; do inst_stop "$i"; done
  sleep 70 # its heartbeat expired, the newest tick is older than a minute
  PC_OPTS=-Doutbox.partitions.maintenanceInterval=2s pc_shop 1001 -Doutbox.shards=64
  kill -CONT "$pid"
  fault "заморозка кончилась: у экземпляра 1003 старое число шардов"
  said() { logs_have 'holds cursors for'; }
  check "$(passes wait_for 30 said)" "ERROR «holds cursors for N shards ...» на старом инстансе"
  refused() { tx_send 1003 'topic=payments&key=k1&tag=normal' | grep -q 'refuses this write'; }
  check "$(passes wait_for 30 refused)" "его запись получает отказ, а не застревает"
  inst_stop 1003
  PC_OPTS=-Doutbox.partitions.maintenanceInterval=2s pc_shop 1003
  pc_load 1001 "$PAY"
  pc_load 1003 "$PAY"
  sleep 8
  REPAIRED="рестарт одного устаревшего инстанса"
  pc_finish --max-dups 0
}

s_inbox_rotation() { # inbox ranges made and dropped under load, on the target and in the source
  pc_begin inbox-rotation
  local tiny='-Doutbox.inbox.partitionInterval=1m -Doutbox.inbox.retention=2m -Doutbox.inbox.premake=3m -Doutbox.partitions.maintenanceInterval=2s'
  TARGET_OPTS=$tiny PC_OPTS=$tiny trio "$MIX"
  local first
  first=$(psql_q "select count(*) from pg_inherits where inhparent = 'inbox'::regclass")
  sleep 150
  local names
  names=$(psql_q "select min(c.relname) from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = 'inbox'::regclass")
  check "$(passes logs_have 'dropped.*inbox_t|inbox_t[0-9]+')" "диапазоны inbox создаются и удаляются (нижний сейчас $names, было диапазонов $first)"
  check "$(passes is "$(logs_count 'SQLState 23514')" = 0)" "ни одной вставки мимо диапазона"
  pc_finish --max-dups 0
}
