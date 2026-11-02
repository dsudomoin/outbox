# Partitions: «Партиции», «Переполнение хвоста», «Осиротевшие партиции», «Застрявшие шарды», «Обслуживание работает там же, где relay».
TINY='-Doutbox.partitions.xidsPerPartition=1000 -Doutbox.partitions.premake=4 -Doutbox.partitions.maintenanceInterval=1s'
overflow() { [ "$(st 1001 'd["tailOverflow"]')" = True ]; }
no_overflow() { [ "$(st 1001 'd["tailOverflow"]')" = False ]; }
writers_failed() { # sum of failed transactions of the given instances
  local i n=0
  for i in "$@"; do n=$((n + $(stats "$i" | j 'sum(d["failedByClass"].values())'))); done
  echo "$n"
}
parts() { psql_q "select count(*) from pg_inherits where inhparent = 'outbox'::regclass"; }
lowest() { psql_q "select min(substring(c.relname from 'outbox_x([0-9]+)')::bigint) from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = 'outbox'::regclass"; }

s_rotation() { # tiny ranges: partitions made and dropped constantly under load and kill -9 during their DDL
  pc_begin rotation
  PC_OPTS=$TINY trio "$MIX"
  local first n
  sleep 5
  first=$(lowest)
  for n in 1 2 3 4 5 6; do
    sleep 7
    pc_kill $((1001 + n % 3))
    sleep 2
    PC_OPTS=$TINY pc_shop $((1001 + n % 3))
  done
  sleep 10
  local last
  last=$(lowest)
  check "$(passes gt "$last" "$first")" "пройденные партиции удаляются: нижний диапазон $first → $last"
  check "$(passes is "$(parts)" -lt 40)" "партиций не накопилось: $(parts)"
  pc_finish --min-dups 1 --max-dups $((6 * BATCH_DUPS))
}

s_tail_overflow() { # a transaction left open defers the swap, the ranges run out: rows go to the tail, no write fails; healed
  pc_begin tail-overflow
  # small enough for the tail to stay under maxTailScan: a bigger one waits for the night window, see tail-big
  PC_OPTS=$TINY trio 'rate=70&writers=4&keys=30&topics=payments'
  sleep 5
  curl -sf -XPOST "$(pub 1001)/tx/hold?ms=60000&topic=payments&key=held&tag=held" > /dev/null
  fault "транзакция с send открыта 60 с: своп откладывается"
  check "$(passes wait_for 55 overflow)" "строки ушли в хвост (tailOverflow)"
  sleep 3
  check "$(passes is "$(writers_failed 1001 1002 1003)" = 0)" "ни одна запись не упала"
  gauge_up() { # a gauge belongs to the instance whose maintenance run saw the tail
    local i
    for i in 1001 1002 1003; do [ "$(on "$i" metric outbox_partitions_overflow)" = 1 ] && return 0; done
    return 1
  }
  check "$(passes wait_for 30 gauge_up)" "gauge outbox.partitions.overflow = 1"
  deferred() { logs_have 'deferred to the next run'; }
  check "$(passes wait_for 30 deferred)" "WARN «deferred» называет держателя, когда он старше 20 с"
  check "$(passes wait_for 90 no_overflow)" "держатель закончил: хвост стал диапазоном сам"
  check "$(passes gt "$(st 1001 'd["premade"]')" 0)" "premade снова > 0"
  pc_finish --max-dups 0 --slow-keys held
}

s_tail_big() { # the tail outgrows maxTailScan: stays hot, ERROR names the window; the window converts it
  pc_begin tail-big
  # the window opens two minutes from now, in UTC
  local from to
  from=$(date -u -v+3M +%H:%M)
  to=$(date -u -v+8M +%H:%M)
  local opts="$TINY -Doutbox.partitions.maxTailScan=65536 -Doutbox.partitions.tailConversion.window=$from-$to -Doutbox.partitions.tailConversion.zone=UTC"
  PC_OPTS=$opts trio 'rate=300&writers=4&keys=30&topics=payments'
  sleep 3
  curl -sf -XPOST "$(pub 1001)/tx/hold?ms=45000&topic=payments&key=held&tag=held" > /dev/null
  check "$(passes wait_for 60 overflow)" "строки ушли в хвост"
  sleep 45
  check "$(passes overflow)" "держатель закончил, но хвост больше maxTailScan и остаётся горячим"
  check "$(passes logs_have 'over outbox.partitions.maxTailScan')" "ERROR «over outbox.partitions.maxTailScan» называет окно"
  check "$(passes is "$(writers_failed 1001 1002 1003)" = 0)" "ни одна запись не упала"
  check "$(passes wait_for 300 no_overflow)" "окно tailConversion превратило его в диапазон"
  pc_finish --max-dups 0 --slow-keys held
}

s_tail_detached() { # someone detaches the tail by hand: the repair is at most a maintenance run away, writes never notice
  pc_begin tail-detached
  trio "$PAY"
  sleep 8
  psql_q 'alter table outbox detach partition outbox_tail' > /dev/null
  fault "хвост отсоединён вручную"
  attached() { [ "$(psql_q "select count(*) from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = 'outbox'::regclass and c.relname = 'outbox_tail'")" = 1 ]; }
  check "$(passes wait_for 30 attached)" "обслуживание вернуло хвост (пустой заменён новым)"
  sleep 5
  check "$(passes is "$(writers_failed 1001 1002 1003)" = 0)" "ни одна запись не упала"
  pc_finish --max-dups 0
}

fullest_range() { # the range holding the most rows, below the one being written
  psql_q "select c.relname from (select tableoid, count(*) n from outbox group by 1) t join pg_class c on c.oid = t.tableoid
    where c.relname ~ '^outbox_x[0-9]+$' and substring(c.relname from 'outbox_x([0-9]+)')::bigint + 1000 < pg_snapshot_xmin(pg_current_snapshot())::text::bigint
    order by t.n desc limit 1"
}

s_orphan_foreign() { # an operator detaches a range with rows nobody delivered: ERROR, gauge, /status; one admin call returns them
  pc_begin orphan-foreign
  PC_OPTS=$TINY trio 'rate=200&writers=4&keys=3000&topics=payments' # several keys per shard: the shard pauses, its rows stay in their range; one key per shard goes to the retry table
  sleep 3
  tfault 'mode=error'
  sleep 12
  local part
  part=$(fullest_range)
  [ -n "$part" ] || { check 1 "нет диапазона со строками"; pc_finish; return 1; }
  psql_q "alter table outbox detach partition $part" > /dev/null
  fault "диапазон $part со строками отсоединён вручную"
  sleep 4
  check "$(passes is "$(st 1001 "any(o['table'] == '$part' for o in d['orphans'])")" = True)" "/status показывает сироту $part"
  check "$(passes logs_have 'ERROR.*'"$part")" "ERROR называет таблицу"
  check "$(passes gt "$(on 1001 metric outbox_partitions_orphaned)" 0)" "gauge outbox.partitions.orphaned > 0"
  tfault 'mode=ok'
  local n=0 code
  until [ "$n" -gt 20 ]; do
    code=$(admin_code 1002 POST "orphans/$part/restore?fromXid=0")
    [ "$code" = 200 ] && break
    n=$((n + 1))
    sleep 2
  done
  check "$(passes is "$code" = 200)" "POST orphans/.../restore: $code (вызовов $((n + 1)))"
  REPAIRED="POST orphans/{table}/restore на живом инстансе"
  pc_finish --exempt-topics payments
}

s_orphan_own() { # a crash between the library's own detach and drop: maintenance moves the rows back by itself
  pc_begin orphan-own
  PC_OPTS=$TINY trio 'rate=200&writers=4&keys=3000&topics=payments' # several keys per shard: the shard pauses, its rows stay in their range; one key per shard goes to the retry table
  sleep 3
  tfault 'mode=error'
  sleep 12
  local part
  part=$(fullest_range)
  [ -n "$part" ] || { check 1 "нет диапазона со строками"; pc_finish; return 1; }
  psql_q "insert into outbox_dropping (table_name) values ('$part'); alter table outbox detach partition $part" > /dev/null
  fault "как падение между detach и drop: $part отсоединён с отметкой"
  tfault 'mode=ok'
  gone() { [ "$(psql_q "select count(*) from pg_class where relname = '$part'")" = 0 ]; }
  check "$(passes wait_for 30 gone)" "обслуживание вернуло строки и удалило таблицу"
  check "$(passes logs_have 'moved [0-9]+ rows')" "INFO «moved N rows»"
  pc_finish --exempt-topics payments
}

s_inbox_orphan() { # a range of the target's inbox detached inside its window: attached back, no double effect
  pc_begin inbox-orphan
  TARGET_OPTS=-Doutbox.partitions.maintenanceInterval=1s trio "$PAY"
  sleep 5
  local part
  part=$(target_q "select c.relname from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = 'inbox'::regclass order by c.relname desc" | while read -r p; do
    [ "$(target_q "select count(*) from $p")" -gt 0 ] && { echo "$p"; break; }
  done)
  [ -n "$part" ] || { check 1 "нет диапазона inbox со строками"; pc_finish; return 1; }
  target_q "alter table inbox detach partition $part" > /dev/null
  fault "диапазон inbox $part отсоединён"
  attached_back() { [ "$(target_q "select count(*) from pg_inherits i join pg_class c on c.oid = i.inhrelid where i.inhparent = 'inbox'::regclass and c.relname = '$part'")" = 1 ]; }
  check "$(passes wait_for 30 attached_back)" "обслуживание inbox присоединило его обратно"
  sleep 5
  pc_finish
}

s_ddl_lock() { # someone's ACCESS EXCLUSIVE on a partition stops delivery for as long; the relay comes back by itself
  pc_begin ddl-lock
  trio "$PAY"
  sleep 8
  local part
  part=$(psql_q "select c.relname from (select tableoid, count(*) n from outbox group by 1) t join pg_class c on c.oid = t.tableoid order by t.n desc limit 1")
  docker exec -d "$PG" psql -U outbox -d outbox -Atq -c "begin; lock table $part in access exclusive mode; select pg_sleep(12); commit" > /dev/null
  fault "ACCESS EXCLUSIVE на $part на 12 с"
  sleep 25
  check "$(passes logs_have 'tick made without its shard set')" "WARN «outbox tick made without its shard set»"
  pc_finish --max-dups 0
}

s_relay_off() { # no instance runs the relay: writes go on, maintenance stops and says so; a rolling change delivers everything
  pc_begin relay-off
  pc_target
  local i
  for i in 1001 1002; do pc_shop "$i" -Doutbox.relay.enabled=false; pc_load "$i" "$PAY"; done
  sleep 25
  check "$(passes is "$(effects)" = 0)" "доставки нет, запись идёт"
  check "$(passes gt "$(st 1001 'int(d.get("maintenanceSinceSuccessSeconds") or 0)')" 10)" "/status: обслуживание давно не работало"
  for i in 1001 1002; do inst_stop "$i"; pc_shop "$i"; sleep 3; done
  REPAIRED="rolling-рестарт с relay.enabled = true"
  pc_finish --max-dups 0
}

s_stranded() { # the tick a shard with rows starts from is gone: one admin call on the running fleet
  pc_begin stranded
  pc_target
  local i
  for i in 1001 1002 1003; do pc_shop "$i"; pc_load "$i" 'rate=100&writers=4&keys=3000&topics=payments'; done
  sleep 8
  tfault 'mode=error'
  sleep 6
  check "$(passes is "$(admin_code 1002 POST recovery/stranded-shards)" = 409)" "пока застрявших нет, вызов отвечает 409"
  psql_q "delete from outbox_tick where id in (select tick_id from (select distinct on (shard) shard, tick_id from outbox_cursor order by shard, seq desc) c where shard in (select distinct shard from outbox))" > /dev/null
  psql_q "delete from outbox_checkpoint" > /dev/null
  fault "тики, с которых стартуют шарды со строками, удалены вручную"
  tfault 'mode=ok'
  said() { logs_have 'start from ticks'; }
  check "$(passes wait_for 30 said)" "ERROR «start from ticks ... that no longer exist»"
  sleep 3
  local n=0 code total=0 body
  until [ "$n" -gt 20 ]; do
    body=$(admin 1002 POST recovery/stranded-shards)
    code=$(echo "$body" | sed -n 1p)
    [ "$code" = 200 ] && total=$(echo "$body" | sed -n 2p | j 'd["count"]')
    [ "$code" = 200 ] && break
    n=$((n + 1))
    sleep 2
  done
  check "$(passes is "$code" = 200)" "POST recovery/stranded-shards на живом инстансе: $code, строк $total (вызовов $((n + 1)))"
  REPAIRED="POST recovery/stranded-shards, без остановки"
  pc_finish --timeout 120 --exempt-topics payments # the rows sent again are new messages, after later ones of their key
}
