# Several outboxes and apps: «Независимые цели», «Несколько инстансов».
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
  pc_courier 1003 $COURIER_MAIN
  pc_courier 1004 $COURIER_MAIN
  local i
  for i in 1001 1002; do pc_load "$i" "$MIX"; done
  for i in 1003 1004; do pc_load "$i" 'rate=100&writers=2&keys=20&topics=deliveries,receipts,routed'; done
  sleep 15
  pc_kill 1001
  pc_kill 1003
  sleep 5
  pc_shop 1001
  pc_courier 1003 $COURIER_MAIN
  sleep 10
  pc_finish
}

s_everything() { # all at once: writers with rollbacks, two apps, poison, requeue, flaky target, kills, a long transaction
  pc_begin everything
  BASES=(outbox courier_main)
  pc_target
  local i
  for i in 1001 1002; do pc_shop "$i"; pc_load "$i" 'rate=100&writers=4&keys=30&topics=payments,ledger,projection&rollbackPercent=5&tags=poison:60'; done
  for i in 1003 1004; do pc_courier "$i" $COURIER_MAIN; pc_load "$i" 'rate=100&writers=4&keys=30&topics=deliveries,receipts,routed&rollbackPercent=5&tags=requeue:40,retry3:50'; done
  tfault 'failPercent=10&dropPercent=5'
  tx_hold 45000
  local n
  for n in 1 2 3 4 5 6; do
    sleep 10
    case $((n % 4)) in
      0) pc_kill 1001; sleep 3; pc_shop 1001 ;;
      1) pc_kill 1003; sleep 3; pc_courier 1003 $COURIER_MAIN ;;
      2) inst_kill9 "$T"; fault "kill -9 цели"; sleep 5; pc_target; tfault 'failPercent=10&dropPercent=5' ;;
      3) conn_kill ;;
    esac
  done
  tfault 'failPercent=0&dropPercent=0'
  pc_finish --min-dups 1 --dead-tags poison --exempt-tags poison,requeue --timeout 300
}

s_inbox_rotation() { # inbox ranges made and dropped under load, on the target and in the source
  pc_begin inbox-rotation
  local tiny='-Doutbox.inboxPartitionInterval=1m -Doutbox.inbox.retention=2m -Doutbox.inboxPremake=3m -Doutbox.maintenanceInterval=2s'
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
