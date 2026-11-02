# Resources: «Размер пула соединений», «max_locks_per_transaction», heartbeat on its own connection.
s_small_pool() { # the README's formula: own load + relay.workers + 3, with writers and an in-process DB handler sharing it
  pc_begin small-pool
  DB_POOL=$((4 + 4 + 3))
  pc_target
  local i
  for i in 1001 1002 1003; do pc_shop "$i"; pc_load "$i" 'rate=150&writers=8&keys=30&topics=payments,ledger,projection'; done
  sleep 40
  local live
  live=$(st 1001 'sum(1 for x in d["instances"] if x["live"])' || echo 0)
  check "$(passes is "$live" = 3)" "heartbeat жив у всех при занятом пуле"
  pc_finish --max-dups 0
}

s_pool_undersized() { # below the formula, and the handler needs a second connection of the same pool
  pc_begin pool-undersized
  DB_POOL=4
  pc_target
  local i
  for i in 1001 1002; do pc_shop "$i" -Ddb.connectionTimeout=1s; pc_load "$i" 'rate=60&writers=2&keys=20&topics=projection'; done
  sleep 25
  local a b
  a=$(effects projection)
  sleep 15
  b=$(effects projection)
  fault "пул меньше формулы: за 15 с эффектов $((b - a))"
  log "undersized pool: effects $a → $b, stats $(stats 1001)"
  for i in 1001 1002; do pc_unload "$i"; done
  if python3 "$PC/scenarios/ledger.py" --source "$PG" --target "$TGT" --base outbox --timeout 150 > /dev/null; then
    check 0 "при пуле меньше формулы доставка медленнее, но сходится сама"
  else
    # the repair: a bigger pool by a rolling restart, one instance at a time
    DB_POOL=24
    for i in 1001 1002; do inst_stop "$i"; pc_shop "$i"; sleep 3; done
    REPAIRED="rolling-рестарт с большим пулом, без остановки всех"
  fi
  pc_finish
}

s_lock_table_full() { # bulk sendAll past max_locks_per_transaction × max_connections: writers fail with 53200, nothing else
  pc_begin lock-table-full
  pc_target
  local i
  for i in 1001 1002 1003; do pc_shop "$i"; pc_load "$i" 'rate=0&writers=3&topics=payments&bulk=true&txMessages=20000&holdMs=6000'; done
  sleep 14
  local failed=0
  for i in 1001 1002 1003; do failed=$((failed + $(stats "$i" | j 'sum(d["failedByClass"].values())'))); done
  check "$(passes gt "$failed" 0)" "писатели падают с out of shared memory, а не зависают: $failed отказов"
  for i in 1001 1002 1003; do pc_unload "$i"; done
  sleep 5
  check "$(passes effects_grow 10)" "доставка идёт, когда таблица блокировок освободилась"
  pc_finish --timeout 300 # a relay step that hits the full table rolls back: duplicates are allowed
}
