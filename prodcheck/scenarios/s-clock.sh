# Clock skew. The README: partitions do not depend on clocks; times in the retry and parked tables are the database's;
# message ids are UUIDv7 of the writer's clock, and the inbox window goes by them.
SKEWED=kora-outbox-prodcheck-skewed # the sender with the skewed clock
SKEWED_TARGET=kora-outbox-prodcheck-skewed-target # the receiver with the skewed clock
ftime() { docker exec "$PG" sh -c "echo '$1' > /tmp/faketime.rc"; fault "часы базы: $1"; }

s_db_clock() { # the database's clock jumps forward and back under load, with a failing key in the retry table
  pc_begin db-clock
  # libfaketime hangs initdb and the image's entry point: only the server runs under it, on a database made without it
  local volume=kora-outbox-prodcheck-backup
  docker stop -t 2 "$PG" > /dev/null
  source_replace "-v $volume:/var/lib/postgresql/data" 55434
  fresh_db "$PG"
  docker exec -i "$PG" psql -U outbox -d outbox -q < "$PC/scenarios/ledger.sql"
  docker stop -t 5 "$PG" > /dev/null
  docker rm "$PG" > /dev/null
  docker run -d --name "$PG" --cpus 2 --memory 2g --shm-size 512m -p 127.0.0.1:55434:5432 -v "$volume:/var/lib/postgresql/data" --entrypoint sh "$PG_IMAGE" \
    -c "echo '+0' > /tmp/faketime.rc; chmod 644 /tmp/faketime.rc; exec gosu postgres env LD_PRELOAD=$FAKETIME_LIB/libfaketime.so.1 FAKETIME_TIMESTAMP_FILE=/tmp/faketime.rc FAKETIME_CACHE_DURATION=1 FAKETIME_DONT_FAKE_MONOTONIC=1 postgres -c max_connections=300 -c shared_buffers=512MB -c max_locks_per_transaction=256" > /dev/null
  wait_for 60 docker exec "$PG" pg_isready -U outbox -d outbox -q
  sleep 2
  PC_PG_URL=jdbc:postgresql://localhost:55434/outbox
  trio "$PAY"
  tfault 'failKeys=k3'
  sleep 10
  local real now
  real=$(date +%s)
  ftime '+1m'
  sleep 3
  now=$(psql_q "select extract(epoch from now())::bigint")
  check "$(passes is "$((now - real))" -gt 50)" "часы базы действительно ушли вперёд: на $((now - real)) с"
  sleep 12
  ftime '+0'
  fault "часы базы вернулись назад на 1 мин: сроки, поставленные за это время, ждут не дольше неё"
  sleep 15
  tfault 'failKeys='
  check "$(passes effects_grow 8)" "доставка идёт после прыжка назад"
  pc_finish --no-time-checks --timeout 150
}

s_app_clock() { # an instance's clock is off by hours (and past the inbox window): one instance in a container with libfaketime
  pc_begin app-clock
  trio "$MIX"
  sleep 5
  local offset
  for offset in +2h -2h +3d; do
    skewed_shop "$offset" app-skewed
    pc_load 1004 "$MIX"
    fault "экземпляр с часами $offset"
    sleep 15
    pc_unload 1004
    docker stop -t 20 "$SKEWED" > /dev/null
  done
  docker rm -f "$SKEWED" > /dev/null 2>&1 || true
  check "$(passes gt "$(on 1001 metric outbox_messages_delivered_total)" 0)" "остальные доставляют"
  log "outside window: $(curl -s localhost:$((8084 + 1001))/metrics | grep -c outside_window)"
  pc_finish --no-time-checks
}

s_app_clock_retry() { # an instance whose clock runs ahead schedules retries and pauses shards, then it is gone: the others must not wait for its clock
  pc_begin app-clock-retry
  pc_target
  skewed_shop +2h app-skewed
  pc_load 1004 'rate=100&writers=2&keys=30&topics=payments'
  sleep 3
  tfault 'mode=error'
  fault "цель лежит; единственный relay — экземпляр с часами +2 ч: он ставит сроки повторов и паузы по своим часам"
  sleep 12
  local due
  due=$(psql_q "select round(extract(epoch from max(next_attempt_at) - clock_timestamp())) from outbox_retry")
  log "the latest retry head is due in $due s by the database's clock"
  pc_unload 1004
  pc_shop 1001
  pc_shop 1002
  docker stop -t 20 "$SKEWED" > /dev/null
  docker rm -f "$SKEWED" > /dev/null 2>&1 || true
  fault "экземпляр с часами ушёл, двое с верными часами; цель вернулась"
  tfault 'mode=ok'
  pc_finish --no-time-checks --timeout 60
}

# The inbox window goes by the JVM clocks of the sender (the id's time) and the receiver (its partitions): every lost
# response makes a second delivery, which only the inbox can stop. The target counts a second effect instead of failing.
skewed_shop() { # skewed_shop OFFSET [INSTANCE]: the sender in a container on ports 9083 and 9088, app-skewedOFFSET by default
  docker rm -f "$SKEWED" > /dev/null 2>&1 || true
  docker run -d --name "$SKEWED" -p 127.0.0.1:9083:9083 -p 127.0.0.1:9088:9088 -v "$PC/shop/build/install/shop-n:/app:ro" \
    -e HTTP_PORT=9083 -e METRICS_PORT=9088 -e INSTANCE_NAME="${2:-app-skewed$1}" \
    -e POSTGRES_URL=jdbc:postgresql://host.docker.internal:55432/outbox \
    -e TARGET_DB_URL=jdbc:postgresql://host.docker.internal:55433/outbox -e TARGET_URL=http://host.docker.internal:9090 \
    -e LD_PRELOAD="$FAKETIME_LIB/libfaketimeMT.so.1" -e FAKETIME="$1" -e FAKETIME_DONT_FAKE_MONOTONIC=1 \
    -e JAVA_OPTS=-Xmx768m kora-outbox-prodcheck-app /app/bin/shop > /dev/null
  wait_for 90 curl -sf localhost:9088/system/readiness
}

skewed_target() { # skewed_target OFFSET: the receiver and its inbox in a container on the target's ports
  docker rm -f "$SKEWED_TARGET" > /dev/null 2>&1 || true
  docker run -d --name "$SKEWED_TARGET" -p 127.0.0.1:9090:9090 -p 127.0.0.1:9095:9095 -v "$PC/target/build/install/target:/app:ro" \
    -e HTTP_PORT=9090 -e METRICS_PORT=9095 -e INSTANCE_NAME="target$1" \
    -e POSTGRES_URL=jdbc:postgresql://host.docker.internal:55433/outbox \
    -e LD_PRELOAD="$FAKETIME_LIB/libfaketimeMT.so.1" -e FAKETIME="$1" -e FAKETIME_DONT_FAKE_MONOTONIC=1 \
    -e JAVA_OPTS=-Xmx768m kora-outbox-prodcheck-app /app/bin/target > /dev/null
  wait_for 90 curl -sf localhost:9095/system/readiness
  tfault 'dropPercent=5&countDoubles=true'
}

doubles() { target_q "select count(*) from pc_double_effect"; }
inbox_logs() { # the target's or the sender's container log joins the scenario's
  docker logs "$1" >> "$LOGS/pc-$SCN-$1.log" 2>&1 || true
}

inbox_verdict() { # stuck keys, 23514, the numbers by offset
  local c23514
  c23514=$(cat "$LOGS/pc-$SCN-"*.log | grep -cE '23514|violates partition constraint|no partition of relation' || true)
  check "$(passes is "$c23514" = 0)" "ни одной вставки inbox мимо партиции (23514): $c23514"
  check "$(passes is "$(doubles)" = 0)" "ни одного второго эффекта: всего $(doubles); по сдвигам: $1"
}

s_inbox_clock_sender() { # the sender's clock is off: the ids' time, so the dedupe window; inside and past retention (24 h)
  pc_begin inbox-clock-sender
  AFTER="docker rm -f $SKEWED"
  pc_target
  tfault 'dropPercent=5&countDoubles=true'
  pc_shop 1001
  pc_load 1001 'rate=20&writers=2&keys=30&topics=payments'
  local offset per="" a b
  for offset in +2h -2h +3d -3d; do
    a=$(doubles)
    skewed_shop "$offset"
    pc_load 1004 'rate=100&writers=2&keys=30&topics=payments'
    fault "отправитель с часами $offset"
    sleep 15
    pc_unload 1004
    sleep 5
    docker stop -t 20 "$SKEWED" > /dev/null
    inbox_logs "$SKEWED"
    b=$(doubles)
    per="$per $offset:$((b - a))"
    log "sender $offset: second effects $((b - a)), outside_window $(curl -s localhost:9095/metrics | grep -E '^outbox_inbox_outside_window' | head -1)"
  done
  docker rm -f "$SKEWED" > /dev/null 2>&1 || true
  tfault 'dropPercent=0'
  inbox_verdict "$per"
  pc_finish --no-time-checks --timeout 120
}

s_inbox_clock_receiver() { # the receiver's clock is off: its partitions and its window; the ids are right
  pc_begin inbox-clock-receiver
  AFTER="docker rm -f $SKEWED_TARGET"
  pc_shop 1001
  local offset per="" a b
  skewed_target +0
  pc_load 1001 'rate=100&writers=2&keys=30&topics=payments'
  for offset in +2h -2h +3d -3d +0; do
    a=$(doubles)
    docker stop -t 10 "$SKEWED_TARGET" > /dev/null
    inbox_logs "$SKEWED_TARGET"
    skewed_target "$offset"
    fault "получатель с часами $offset"
    sleep 15
    b=$(doubles)
    per="$per $offset:$((b - a))"
    log "receiver $offset: second effects $((b - a)), partitions $(target_q "select count(*) from pg_inherits where inhparent = 'inbox'::regclass"), outside_window $(curl -s localhost:9095/metrics | grep -E '^outbox_inbox_outside_window' | head -1)"
  done
  check "$(passes effects_grow 5)" "доставка идёт после всех сдвигов"
  tfault 'dropPercent=0'
  inbox_logs "$SKEWED_TARGET"
  inbox_verdict "$per"
  pc_finish --no-time-checks --timeout 120
}
