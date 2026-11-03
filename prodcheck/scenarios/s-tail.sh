# A long tail of one key: «Обработка сбоев», «Отложить запись». Real rows through the real paths: a failing key's followers
# in the retry table, a key parked for as long as its block lasts. The numbers are the result; EXPLAIN goes to build/results.
retry_size() { psql_q 'select count(*) from outbox_retry'; }
parked_size() { psql_q 'select count(*) from outbox_parked'; }

tail_batch_stats() { # the mean of outbox.batch.duration over the last N seconds, in ms, instance I
  local i=$1 n=$2 c0 s0 c1 s1
  c0=$(on "$i" metric outbox_batch_duration_seconds_count); s0=$(curl -s "localhost:$((8084 + i))/metrics" | awk '$1 ~ /^outbox_batch_duration_seconds_sum/ {s += $2} END {printf "%.3f", s}')
  sleep "$n"
  c1=$(on "$i" metric outbox_batch_duration_seconds_count); s1=$(curl -s "localhost:$((8084 + i))/metrics" | awk '$1 ~ /^outbox_batch_duration_seconds_sum/ {s += $2} END {printf "%.3f", s}')
  python3 -c "print(f'{($s1 - $s0) * 1000 / max($c1 - $c0, 1):.1f} ms per step, {($c1 - $c0) / $n:.1f} steps/s')"
}

tail_snapshot() { # what writers and the relay are doing, when a phase does not move
  log "snapshot $1: sent $(psql_q "select key, count(*) from pc_sent where key like 'tail-%' group by 1" | tr '\n' ' '), retry $(retry_size), parked $(parked_size), stats $(stats 1001 | cut -c1-200)"
  psql_q "select state, wait_event_type, wait_event, round(extract(epoch from clock_timestamp() - query_start)), left(query, 70) from pg_stat_activity where datname = 'outbox' and pid <> pg_backend_pid() and state <> 'idle' order by 4 desc limit 12" >&2 || true
}

tail_disk() { # tail_disk LABEL: the size and the update counters of both tables, into the result and build/results
  local line
  sleep 2 # the backends' table statistics reach the view about once a second
  line=$(psql_q "select string_agg(format('%s: heap %s MB, total %s MB, updates %s, hot %s, dead %s, autovacuums %s', relname,
      round(pg_relation_size(relid) / 1048576.0, 1), round(pg_total_relation_size(relid) / 1048576.0, 1),
      n_tup_upd, n_tup_hot_upd, n_dead_tup, autovacuum_count), '; ' order by relname)
    from pg_stat_user_tables where relname in ('outbox_parked', 'outbox_retry')")
  log "disk $1: live retry $(retry_size), parked $(parked_size); $line"
  echo "$(date -u +%T) $1: live retry $(retry_size), parked $(parked_size); $line" >> "$RESULT_DIR/retry-long-tail.disk"
  check 0 "диск, $1 (retry $(retry_size), parked $(parked_size)): $line"
}

auto_explain_on() { # the plans and the row counts of the library's own statements, with their real parameters, in the server log
  docker exec "$PG" psql -U outbox -d outbox -Atq -c "load 'auto_explain'" \
    -c "alter system set session_preload_libraries = 'auto_explain'" \
    -c "alter system set auto_explain.log_min_duration = '5ms'" \
    -c "alter system set auto_explain.log_analyze = on" \
    -c "alter system set auto_explain.log_timing = off" \
    -c "alter system set auto_explain.log_buffers = on" > /dev/null
  psql_q "select pg_reload_conf()" > /dev/null
}

auto_explain_report() { # per statement of interest: calls over the threshold, the slowest, rows written; the slowest plan goes to the file
  docker logs "$PG" 2>&1 | STAMP="$(date -u +%T)" python3 -c '
import os, re, sys
text = sys.stdin.read()
blocks = re.split(r"\n(?=[0-9-]+ [0-9:.]+ \w+ \[\d+\] LOG:  duration: )", text)
kinds = {"postponeNewer": "update outbox_parked p set due_at", "selectOlder": "in_retry", "repark": "update outbox_parked set requeues",
         "putOff": "update outbox_parked set due_at", "selectDue parked": "from outbox_parked where shard = $1 and due_at <=",
         "minDue parked": "select min(due_at) from outbox_parked"}
out, slowest = [], {}
for name, needle in kinds.items():
    hits = [b for b in blocks if "duration:" in b and needle in b]
    ms = [float(re.search(r"duration: ([0-9.]+) ms", b).group(1)) for b in hits]
    rows = [int(x) for b in hits for x in re.findall(r"actual rows=(\d+)", b)] # an Update node itself reports 0
    if ms:
        out.append(f"{name}: {len(ms)} calls over 5 ms, slowest {max(ms):.0f} ms, total {sum(ms)/1000:.1f} s, rows in a node up to {max(rows) if rows else 0}")
        slowest[name] = hits[ms.index(max(ms))]
    else:
        out.append(f"{name}: no call over 5 ms")
print("; ".join(out))
open(os.environ["PLANS"], "w").write("\n\n".join(f"== {k}\n{v}" for k, v in slowest.items()))
' 
}

mate_latency() { # mate_latency LABEL SHARDS FROM TO: delivery latency of the other keys on those shards, sent in that window
  local mates sent done
  mates=$(psql_q "select string_agg(distinct format('(%L,%L)', topic, convert_from(key, 'UTF8')), ',') from outbox where shard in ($2) and convert_from(key, 'UTF8') not like 'tail-%'")
  if [ -z "$mates" ]; then log "$1: no other key on shard $2"; return 0; fi
  sent=$(psql_q "copy (select message_id, extract(epoch from at) from pc_sent where (topic, key) in ($mates) and at >= to_timestamp($3) and at <= to_timestamp($4)) to stdout with csv")
  done=$(target_q "copy (select message_id, extract(epoch from at) from pc_effect where (topic, key) in ($mates)) to stdout with csv")
  SENT=$sent DONE=$done LABEL=$1 python3 - >&2 << 'PY_MATES'
import os
rows = lambda v: dict(l.split(",") for l in os.environ[v].splitlines() if l)
sent, done = rows("SENT"), rows("DONE")
lat = sorted(float(done[i]) - float(t) for i, t in sent.items() if i in done)
missing = sum(1 for i in sent if i not in done)
pct = lambda q: round(lat[min(int(len(lat) * q), len(lat) - 1)], 3) if lat else None
print(f"{os.environ['LABEL']}: shard mates' messages {len(sent)}, not yet delivered {missing}, p50 {pct(.5)} s, p99 {pct(.99)} s, max {pct(1)} s")
PY_MATES
}

s_retry_long_tail() {
  pc_begin retry-long-tail
  pc_target
  local opts='-Doutbox.relay.batchSize=1000' i rows=100000 out=$RESULT_DIR/retry-long-tail.explain
  AFTER='"${COMPOSE[@]}" --profile failover --profile pooler down -v' # the server settings below go with the stand
  rm -f "$RESULT_DIR/retry-long-tail.disk"
  auto_explain_on
  for i in 1001 1002; do pc_courier "$i" $opts; done
  hctl 1001 'route=main&failKeys=tail-f&waiting=true'
  hctl 1002 'route=main&failKeys=tail-f&waiting=true'
  # neighbours, some on the shard of a tail; a failing key's backlog in the main stream, and how fast it clears the way
  pc_load 1002 'rate=100&writers=2&keys=300&topics=deliveries,receipts'
  sleep 3
  local t0=$(now) written moved=0
  pc_load 1001 'rate=20000&writers=1&txMessages=500&bulk=true&bulkKey=tail-f&topics=deliveries'
  sleep 3
  pc_unload 1001
  written=$(psql_q "select count(*) from pc_sent where key = 'tail-f'")
  all_moved() { [ "$(psql_q "select count(*) from outbox_retry where key = convert_to('tail-f', 'UTF8')")" -ge "$written" ]; }
  check "$(passes wait_for 240 all_moved)" "бэклог сбойного ключа ушёл в retry-таблицу: $written строк"
  moved=$(($(now) - t0))
  log "a failing key's backlog: $written rows out of the main stream in $moved s, about $((written / (moved > 0 ? moved : 1))) rows/s"
  sleep 3
  local fshard
  fshard=$(psql_q "select shard from outbox_retry where key = convert_to('tail-f', 'UTF8') limit 1")
  mate_latency "behind the failing key's backlog" "$fshard" "$t0" "$(now)"
  tail_disk "до хвоста"
  local t=$(now)
  # the retry tail behind a waiting head: no pause, whole batches
  tx_send 1001 'topic=deliveries&key=tail-r&tag=wait' > /dev/null
  # the two keys through sendAll, their seqs consecutive: the order check holds over the whole tail
  pc_load 1001 'rate=3000&writers=1&txMessages=500&bulk=true&bulkKey=tail-r&topics=deliveries'
  pc_load 1002 'rate=3000&writers=1&txMessages=500&bulk=true&bulkKey=tail-p&topics=receipts&tags=hold:1'
  full() { [ "$(psql_q "select count(*) from pc_sent where key = '$1'")" -ge "$rows" ]; }
  ( for n in 10 30 60 120; do command sleep "$n"; tail_snapshot "+$n"; done ) &
  local snap=$!
  full tail-r || wait_for 120 full tail-r || { tail_snapshot "tail-r not written"; thread_dump 1001 stuck; exit 1; }
  pc_unload 1001
  wait_for 120 full tail-p || { tail_snapshot "tail-p not written"; thread_dump 1002 stuck; exit 1; }
  pc_unload 1002
  kill "$snap" 2> /dev/null || true
  log "written in $(($(now) - t)) s"
  settled() { [ "$(psql_q "select count(*) from outbox_retry where key = convert_to('tail-r', 'UTF8')")" -ge "$rows" ] && [ "$(parked_size)" -ge "$((rows - 200))" ]; }
  local fill=$(now)
  check "$(passes wait_for 300 settled)" "хвост на месте через $(($(now) - fill)) с: retry $(retry_size), parked $(parked_size)"
  tail_disk "хвост на месте"
  # others flow beside it
  local from=$(now)
  pc_load 1001 'rate=100&writers=2&keys=30&topics=deliveries,receipts'
  sleep 5
  local steps1
  steps1=$(tail_batch_stats 1001 30)
  log "beside the tail: $steps1"
  local to=$(now)
  # the keys that share a shard with a tail wait behind its rows in id order until they are moved: named, apart from the rest
  local mates
  mates=$(psql_q "select string_agg(distinct convert_from(key, 'UTF8'), ',') from outbox where shard in (select distinct shard from outbox_retry union select distinct shard from outbox_parked) and convert_from(key, 'UTF8') not like 'tail-%'")
  log "shard mates of the tails: ${mates:-none}"
  mate_latency "beside the tails" "$(psql_q "select string_agg(distinct shard::text, ',') from (select shard from outbox_retry where key = convert_to('tail-r', 'UTF8') union select shard from outbox_parked) s")" "$from" "$to"
  pc_unload 1001
  tail_disk "после минуты рядом с хвостом"
  local plans
  plans=$(PLANS=$RESULT_DIR/retry-long-tail.autoexplain auto_explain_report)
  log "auto_explain: $plans"
  check 0 "планы из auto_explain, настоящие вызовы библиотеки: $plans"
  # the plans, with the tail in place and real values
  {
    local s
    s=$(psql_q "select shard from outbox_parked where key = convert_to('tail-p', 'UTF8') limit 1")
    local r
    r=$(psql_q "select shard from outbox_retry where key = convert_to('tail-r', 'UTF8') limit 1")
    local below
    below=$(psql_q "select max(id) from outbox_parked")
    echo "== selectDue (parked)"
    psql_q "explain (analyze, buffers) select id, shard, message_id, topic, key, payload, headers, created_at from outbox_parked where shard = $s and due_at <= now() order by id limit 100"
    echo "== selectOlder (parked, in_retry and head_due lateral), key parked"
    psql_q "explain (analyze, buffers) select p.id, due_at, in_retry, head_due from unnest(array['receipts']::text[], array[convert_to('tail-p', 'UTF8')]::bytea[], array[$below]::int8[]) u(t, b, below)
      cross join lateral (select count(*) > 0 in_retry, min(r.next_attempt_at) head_due from outbox_retry r where r.shard = $s and r.topic = u.t and r.key = u.b) k
      cross join lateral (select id, due_at from outbox_parked where shard = $s and topic = u.t and key = u.b and id < u.below and (not k.in_retry or due_at > now()) order by id limit case when k.in_retry then 1 else 100 end) p order by in_retry desc, id limit 101"
    echo "== selectOlder's head_due on the key with 100k retry rows"
    psql_q "explain (analyze, buffers) select count(*) > 0 in_retry, min(r.next_attempt_at) head_due from outbox_retry r where r.shard = $r and r.topic = 'deliveries' and r.key = convert_to('tail-r', 'UTF8')"
    echo "== selectOfKeys (parked)"
    psql_q "explain (analyze, buffers) select p.id from (select distinct t, b from unnest(array['receipts']::text[], array[convert_to('tail-p', 'UTF8')]::bytea[]) u(t, b)) k cross join lateral (select id from outbox_parked where shard = $s and topic = k.t and key = k.b order by id limit 100) p order by id limit 100"
    echo "== minDue (parked)"
    psql_q "explain (analyze, buffers) select min(due_at) from outbox_parked where shard = $s"
    echo "== due heads (retry)"
    psql_q "explain (analyze, buffers) select id from outbox_retry where shard = $r and next_attempt_at <= now() order by waiting_since is not null, id limit 100"
    echo "== selectOlder on the key with 100k retry rows, by hand"
    psql_q "explain (analyze, buffers) select p.id, due_at, in_retry, head_due from unnest(array['deliveries']::text[], array[convert_to('tail-r', 'UTF8')]::bytea[], array[$(psql_q "select max(id) from outbox_retry")]::int8[]) u(t, b, below)
      cross join lateral (select count(*) > 0 in_retry, min(r.next_attempt_at) head_due from outbox_retry r where r.shard = $r and r.topic = u.t and r.key = u.b) k
      cross join lateral (select id, due_at from outbox_parked where shard = $r and topic = u.t and key = u.b and id < u.below and (not k.in_retry or due_at > now()) order by id limit case when k.in_retry then 1 else 1000 end) p order by in_retry desc, id limit 1001"
    echo "== postponeNewer on the parked tail, by hand, every row due, rolled back"
    local above
    above=$(psql_q "select id from outbox_parked where key = convert_to('tail-p', 'UTF8') order by id offset 1000 limit 1")
    docker exec "$PG" psql -U outbox -d outbox -Atq -c "begin" -c "explain (analyze, buffers) update outbox_parked p set due_at = now() + interval '5 seconds' from unnest(array['receipts']::text[], array[convert_to('tail-p', 'UTF8')]::bytea[], array[$above]::int8[]) k(t, b, above) where p.shard = $s and p.topic = k.t and p.key = k.b and p.id > k.above and p.due_at <= now() + interval '1 day'" -c "rollback"
    echo "== retry_due_at minimum"
    psql_q "explain (analyze, buffers) select min(next_attempt_at) from outbox_retry where shard = $r"
    echo "== retry rows behind a head of a key"
    psql_q "explain (analyze, buffers) select id from outbox_retry where shard = $r and topic = 'deliveries' and key = convert_to('tail-r', 'UTF8') order by id limit 100"
    echo "== size(parked), size(retry): every status and gauge read"
    psql_q "explain (analyze) select count(*) from outbox_parked"
    psql_q "explain (analyze) select count(*) from outbox_retry"
  } > "$out" 2>&1
  grep -E '^==|Execution Time' "$out" >&2
  grep -E '^==|Execution Time|Update on|rows=[0-9]+ loops' "$out" | grep -A3 -E '== (postponeNewer|selectOlder on the key)' | cut -c1-150 >&2 || true
  # the rest of the proof: the tails empty, in order, nothing lost
  local drained=$(now)
  for i in 1001 1002; do hctl "$i" 'route=main&failKeys=&waiting=false&oneRound=true'; done
  for n in 1 2 3 4 5 6; do
    sleep 10
    log "draining: retry $(retry_size), parked $(parked_size), steps of 1001 $(tail_batch_stats 1001 1)"
  done
  tails_empty() { [ "$(psql_q "select (select count(*) from outbox_retry) + (select count(*) from outbox_parked)")" = 0 ]; }
  wait_for 400 tails_empty || true
  tail_disk "хвосты разобраны"
  pc_finish --exempt-tags none --slow-keys "tail-r,tail-p,$mates" --slow-from "$from" --slow-to "$to" --timeout 400
}
