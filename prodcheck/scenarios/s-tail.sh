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

s_retry_long_tail() {
  pc_begin retry-long-tail
  pc_target
  local opts='-Doutbox.relay.batchSize=100' i rows=100000 out=$RESULT_DIR/retry-long-tail.explain
  for i in 1001 1002; do pc_courier "$i" $opts; done
  hctl 1001 'route=main&failKeys=tail-r&waiting=true'
  hctl 1002 'route=main&failKeys=tail-r&waiting=true'
  local t=$(now)
  # the two keys through sendAll, their seqs consecutive: the order check holds over the whole tail
  pc_load 1001 'rate=0&writers=1&txMessages=500&bulk=true&bulkKey=tail-r&topics=deliveries'
  pc_load 1002 'rate=0&writers=1&txMessages=500&bulk=true&bulkKey=tail-p&topics=receipts&tags=hold:1'
  full() { [ "$(psql_q "select count(*) from pc_sent where key = '$1'")" -ge "$rows" ]; }
  wait_for 300 full tail-r
  pc_unload 1001
  wait_for 300 full tail-p
  pc_unload 1002
  log "written in $(($(now) - t)) s"
  settled() { [ "$(retry_size)" -ge "$((rows - 200))" ] && [ "$(parked_size)" -ge "$((rows - 200))" ]; }
  local fill=$(now)
  check "$(passes wait_for 300 settled)" "хвост на месте через $(($(now) - fill)) с: retry $(retry_size), parked $(parked_size)"
  # others flow beside it
  local from=$(now)
  pc_load 1001 'rate=100&writers=2&keys=30&topics=deliveries,receipts'
  sleep 5
  local steps1
  steps1=$(tail_batch_stats 1001 30)
  log "beside the tail: $steps1"
  local to=$(now)
  pc_unload 1001
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
    echo "== retry_due_at minimum"
    psql_q "explain (analyze, buffers) select min(next_attempt_at) from outbox_retry where shard = $r"
    echo "== retry rows behind a head of a key"
    psql_q "explain (analyze, buffers) select id from outbox_retry where shard = $r and topic = 'deliveries' and key = convert_to('tail-r', 'UTF8') order by id limit 100"
    echo "== size(parked), size(retry): every status and gauge read"
    psql_q "explain (analyze) select count(*) from outbox_parked"
    psql_q "explain (analyze) select count(*) from outbox_retry"
  } > "$out" 2>&1
  grep -E '^==|Execution Time' "$out" >&2
  # the rest of the proof: the tails empty, in order, nothing lost
  local drained=$(now)
  for i in 1001 1002; do hctl "$i" 'route=main&failKeys=&waiting=false&oneRound=true'; done
  for n in 1 2 3 4 5 6; do
    sleep 10
    log "draining: retry $(retry_size), parked $(parked_size), steps of 1001 $(tail_batch_stats 1001 1)"
  done
  pc_finish --exempt-tags none --slow-keys tail-r,tail-p --slow-from "$from" --slow-to "$to" --timeout 400
}
