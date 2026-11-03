# The chaos soak: shop and accounts on one Postgres, two outboxes, moderate rates, a repeating cycle of faults for
# SOAK_MINUTES (soak.sh sets it). No fault here touches xids. Not in run-all's default list; see ../README.md.
SOAK_SHOP='rate=60&writers=2&keys=30&topics=payments,ledger,projection&rollbackPercent=5'
SOAK_ACC='rate=40&writers=2&keys=30&tags=block:97,unblock:101,wait:7&rollbackPercent=3'
SOAK_PARTS='-Doutbox.partitions.xidsPerPartition=100000' # a few minutes a partition at these rates: rotation runs throughout
SOAK_BATCH=${SOAK_BATCH:-100} # relay.batchSize of accounts: the random blocks of SOAK_ACC take some keys past it, with a limit of their own
SOAK_ACC_OPTS="$RECOMMENDED $SOAK_PARTS -Doutbox.relay.batchSize=$SOAK_BATCH"
SOAK_BLOCKED='k30 k31 k32 k33 k34' # accounts the load does not touch: only this cycle's unblock ends their block
SOAK_STEPS='kill_one kill_all_but_one rolling pg_restart pg_pause conn_kills target_outage poison row_locks requeue_block'
SOAK_GAP=30 # seconds of plain load after each fault

# One outbox's columns of the series. $1 column prefix, $2 schema, $3 base table.
soak_columns() { echo "$1_mb $1_retry_rows $1_retry_mb $1_parked_rows $1_parked_mb $1_dead $1_dead_tup $1_parts $1_sent"; }
soak_select() {
  local q="$2.$3"
  echo "(select round(coalesce(sum(pg_total_relation_size(c.oid)), 0) / 1048576.0, 1) from pg_class c join pg_namespace n on n.oid = c.relnamespace
      where n.nspname = '$2' and c.relkind = 'r' and c.relname like '$3\\_%'),
    (select count(*) from ${q}_retry), round(pg_total_relation_size('${q}_retry') / 1048576.0, 1),
    (select count(*) from ${q}_parked), round(pg_total_relation_size('${q}_parked') / 1048576.0, 1),
    (select count(*) from ${q}_dead),
    (select coalesce(sum(n_dead_tup), 0) from pg_stat_user_tables where schemaname = '$2' and relname like '$3\\_%'),
    (select count(*) from pg_inherits where inhparent = '$q'::regclass),
    (select count(*) from $2.pc_sent)"
}
SOAK_COLUMNS="db_mb $(soak_columns shop) $(soak_columns acc) acc_effects shop_effects"
SOAK_SQL="set lock_timeout = '5s'; select round(pg_database_size(current_database()) / 1048576.0, 1), $(soak_select shop public outbox), $(soak_select acc accounts account_events),
  (select count(*) from accounts.pc_effect), (select count(*) from public.pc_effect)"

old_gen_mb() { # old_gen_mb I: the old generation in use on instance I's JVM; - when it is not there
  local pid mb=""
  pid=$(cat "$LOGS/app-$1.pid" 2> /dev/null) || pid=""
  [ -z "$pid" ] || mb=$("$JAVA_HOME/bin/jstat" -gc "$pid" 2> /dev/null | awk 'NR == 1 { for (c = 1; c <= NF; c++) if ($c == "OU") u = c } NR == 2 { printf "%.0f", $u / 1024 }') || mb=""
  echo "${mb:--}"
}

soak_row() { # one line of the series; a Postgres that does not answer leaves its columns as -
  local row target i heap=""
  row=$(psql_q "$SOAK_SQL" 2> /dev/null | tr '|' '\t') || row=""
  target=$(target_q 'select count(*) from pc_effect' 2> /dev/null) || target=0
  if [ -n "$row" ]; then
    row=$(awk -F'\t' -v OFS='\t' -v t="$target" '{ $NF = $NF + t; print }' <<< "$row") # shop's sinks: the source's and the target's
  else
    row=$(echo "$SOAK_COLUMNS" | sed 's/[^ ][^ ]*/-/g' | tr ' ' '\t')
  fi
  for i in 1001 1002 1003 1004 1005; do heap="$heap	$(old_gen_mb "$i")"; done
  printf '%s\t%s\t%s\t%s%s\n' "$((($(now) - T0) / 60))" "$(date -u +%T)" "$(cat "$SOAK_PHASE")" "$row" "$heap" >> "$SERIES"
}

soak_series() { # every minute, on the minute of the run
  printf 'min\tat\tphase\t%s\theap_1001\theap_1002\theap_1003\theap_1004\theap_1005\n' "$(echo "$SOAK_COLUMNS" | tr ' ' '\t')" > "$SERIES"
  ( while :; do soak_row; command sleep $((60 - ($(now) - T0) % 60)); done ) &
  SERIES_PID=$!
  AFTER="$AFTER; kill $SERIES_PID"
}

soak_phase() { echo "$1" > "$SOAK_PHASE"; }

soak_restart() { # whatever is down comes back; a JVM that went without being killed here is a finding
  local i
  for i in 1001 1002 1003 1004 1005; do
    if up "$i" && ! ours "${APP_PIDS[$i]}"; then
      check 1 "экземпляр $i умер сам, $(date -u +%T): см. лог"
      APP_PIDS[i]=""
    fi
    up "$i" && continue
    if [ "$i" -le 1003 ]; then pc_shop "$i"; else acc "$i" $SOAK_ACC_OPTS; fi
  done
}

soak_kill_one() {
  pc_kill $((1001 + $1 % 3))
  pc_kill $((1004 + $1 % 2))
  sleep 10
}

soak_kill_all_but_one() {
  local i
  for i in 1001 1002 1003; do [ "$i" = $((1001 + $1 % 3)) ] || pc_kill "$i"; done
  pc_kill $((1004 + ($1 + 1) % 2))
  sleep 30
}

soak_rolling() {
  roll "$SHOP" 1001 1002 1003
  local i
  for i in 1004 1005; do
    inst_stop "$i"
    fault "выкатка: экземпляр $i"
    acc "$i" $SOAK_ACC_OPTS
    sleep 4
  done
}

soak_pg_restart() {
  fault "docker restart Postgres"
  docker restart -t 10 "$PG" > /dev/null
}

soak_pg_pause() {
  fault "docker pause Postgres на 20 с"
  docker pause "$PG" > /dev/null
  sleep 20
  docker unpause "$PG" > /dev/null
}

soak_conn_kills() {
  local n
  for n in 1 2 3; do
    fault "pg_terminate_backend всех соединений приложений"
    psql_q "select count(pg_terminate_backend(pid)) from pg_stat_activity where application_name like 'app-%'" > /dev/null
    sleep 5
  done
}

soak_target_outage() {
  inst_kill9 "$T"
  fault "kill -9 цели на 30 с"
  sleep 30
  pc_target
  tfault 'delayMs=5'
}

soak_poison() { # records that never go: both outboxes' keys must pass them, each ends a dead letter
  local n
  for n in 1 2 3 4 5; do
    tx_send 1001 "topic=payments&key=k$((n * 5))&tag=poison" > /dev/null
    tx_send 1004 "topic=$ACC&key=k$((n * 5))&tag=corrupt&raw=garbage" > /dev/null
  done
  fault "по пять записей, которые не доставятся, в оба outbox"
}

soak_row_locks() {
  row_lock 40
  sleep 42
}

soak_requeue_block() { # fewer parked rows per key than a batch: such a key's order is strict
  local a n
  for a in $SOAK_BLOCKED; do tx_send 1004 "topic=$ACC&key=$a&tag=block" > /dev/null; done
  for n in $(seq 1 20); do
    for a in $SOAK_BLOCKED; do tx_send 1005 "topic=$ACC&key=$a&tag=wait" > /dev/null; done
  done
  fault "блокировка пяти счетов, на каждом 20 записей в requeue, 30 с"
  sleep 30
  for a in $SOAK_BLOCKED; do tx_send 1004 "topic=$ACC&key=$a&tag=unblock" > /dev/null; done
}

soak_settled() { # after the drain: the series' last rows and the verdict on size, till autovacuum has had its turn
  kill "$SERIES_PID" 2> /dev/null || true
  soak_phase drained
  local n line rc report=0
  for n in $(seq 1 12); do
    soak_row
    python3 "$PC/scenarios/soak_report.py" "$SERIES" > /dev/null && break
    command sleep 15
  done
  python3 "$PC/scenarios/soak_report.py" "$SERIES" > "$RESULT_DIR/soak.report" || report=$?
  while IFS='|' read -r rc line; do check "$rc" "$line"; done < "$RESULT_DIR/soak.report"
  [ "$report" = 0 ] || grep -q '^1|' "$RESULT_DIR/soak.report" || check 1 "soak_report.py не отработал: см. soak.log"
}

s_soak() {
  local minutes=${SOAK_MINUTES:-60} cycle=0 step i
  SOAK_PHASE=$RESULT_DIR/soak.phase
  SERIES=$RESULT_DIR/soak.series
  soak_phase start
  acc_begin soak
  parked_watch
  docker exec -i "$PG" psql -U outbox -d outbox -q -v ON_ERROR_STOP=1 < "$PC/scenarios/ledger.sql" # shop's, in public
  acc_q "insert into subscription (account, id) select 'k' || g, gen_random_uuid() from generate_series(30, 34) g" > /dev/null
  PC_OPTS=$SOAK_PARTS
  trio "$SOAK_SHOP"
  for i in 1004 1005; do acc "$i" $SOAK_ACC_OPTS; acc_load "$i" "$SOAK_ACC"; done
  soak_phase warmup
  soak_series
  sleep 60
  local end=$(($(now) + minutes * 60))
  while [ "$(now)" -lt "$end" ]; do
    cycle=$((cycle + 1))
    for step in $SOAK_STEPS; do
      [ "$(now)" -lt "$end" ] || break
      soak_phase "$cycle:$step"
      "soak_$step" "$cycle"
      soak_restart
      soak_phase "$cycle:settle"
      sleep "$SOAK_GAP"
    done
  done
  soak_phase drain
  for i in 1001 1002 1003 1004 1005; do if up "$i"; then pc_unload "$i"; fi; done
  unblock_all 1004
  sleep 3
  unblock_all 1004
  local led rc=0
  led=$(python3 "$PC/scenarios/ledger.py" --source "$PG" --target "" --search-path accounts,public --base accounts.account_events \
    --dead-tags corrupt --exempt-tags wait,corrupt --order-within-tag wait --parked-file "$RESULT_DIR/soak.parked" --parked-limit "$SOAK_BATCH" --timeout 300) || rc=$?
  echo "$led" > "$RESULT_DIR/soak.accounts-ledger"
  check "$rc" "accounts: $(echo "$led" | j '{k: d[k] for k in ("sent", "effects", "dead", "lost", "double_effects", "order_violations", "past_limit_first_arrivals", "overtakes", "duplicates", "latency_p99", "drain_seconds", "violations")}')"
  check 0 "циклов: $cycle, минут: $minutes"
  # what pc_finish reads is the shop's: its ledger makes the table's row, accounts' is the check above
  BASES=(outbox)
  LEDGER_ARGS=()
  NO_ADMIN=""
  AFTER_LEDGER='acc_checks; soak_settled'
  pc_finish --dead-tags poison --exempt-tags poison --timeout 300
}
