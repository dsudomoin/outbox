# Clock skew. The README: partitions do not depend on clocks; times in the retry and parked tables are the database's;
# message ids are UUIDv7 of the writer's clock, and the inbox window goes by them.
FAKE=/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so.1
ftime() { docker exec "$PG" sh -c "echo '$1' > /tmp/faketime.rc"; fault "часы базы: $1"; }

s_db_clock() { # the database's clock jumps forward and back under load, with a failing key in the retry table
  pc_begin db-clock
  # libfaketime hangs initdb and the image's entry point: only the server runs under it, on a database made without it
  local volume=kora-outbox-prodcheck-backup
  docker stop -t 2 "$PG" > /dev/null
  source_replace "-v $volume:/var/lib/postgresql/data" 55434
  docker exec "$PG" psql -U outbox -d postgres -Atq -c "drop database if exists outbox with (force)" -c "create database outbox"
  docker exec -i "$PG" psql -U outbox -d outbox -q < "$PC/scenarios/ledger.sql"
  docker stop -t 5 "$PG" > /dev/null
  docker rm "$PG" > /dev/null
  docker run -d --name "$PG" --cpus 2 --memory 2g --shm-size 512m -p 127.0.0.1:55434:5432 -v "$volume:/var/lib/postgresql/data" --entrypoint sh "$PG_IMAGE" \
    -c "echo '+0' > /tmp/faketime.rc; chmod 644 /tmp/faketime.rc; exec gosu postgres env LD_PRELOAD=$FAKE FAKETIME_TIMESTAMP_FILE=/tmp/faketime.rc FAKETIME_CACHE_DURATION=1 FAKETIME_DONT_FAKE_MONOTONIC=1 postgres -c max_connections=300 -c shared_buffers=512MB -c max_locks_per_transaction=256" > /dev/null
  ready() { docker exec "$PG" pg_isready -U outbox -d outbox -q; }
  wait_for 60 ready
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
  local name=kora-outbox-prodcheck-skewed
  skewed_up() { # skewed_up OFFSET: the instance in a Linux container on ports 9083 and 9088
    docker rm -f "$name" > /dev/null 2>&1 || true
    docker run -d --name "$name" -p 127.0.0.1:9083:9083 -p 127.0.0.1:9088:9088 -v "$PC/shop/build/install/shop-n:/app:ro" \
      -e HTTP_PORT=9083 -e METRICS_PORT=9088 -e INSTANCE_NAME=app-skewed \
      -e POSTGRES_URL=jdbc:postgresql://host.docker.internal:55432/outbox \
      -e TARGET_DB_URL=jdbc:postgresql://host.docker.internal:55433/outbox -e TARGET_URL=http://host.docker.internal:9090 \
      -e LD_PRELOAD=/usr/lib/aarch64-linux-gnu/faketime/libfaketimeMT.so.1 -e FAKETIME="$1" -e FAKETIME_DONT_FAKE_MONOTONIC=1 \
      -e JAVA_OPTS=-Xmx768m kora-outbox-prodcheck-app /app/bin/shop > /dev/null
    wait_for 90 curl -sf localhost:9088/system/readiness
    pc_load 1004 "$MIX"
  }
  local offset
  for offset in +2h -2h +3d; do
    skewed_up "$offset"
    fault "экземпляр с часами $offset"
    sleep 15
    pc_unload 1004
    docker stop -t 20 "$name" > /dev/null
  done
  docker rm -f "$name" > /dev/null 2>&1 || true
  check "$(passes gt "$(on 1001 metric outbox_messages_delivered_total)" 0)" "остальные доставляют"
  log "outside window: $(curl -s localhost:$((8084 + 1001))/metrics | grep -c outside_window)"
  pc_finish --no-time-checks
}

s_app_clock_retry() { # an instance whose clock runs ahead schedules retries and pauses shards, then it is gone: the others must not wait for its clock
  pc_begin app-clock-retry
  pc_target
  local name=kora-outbox-prodcheck-skewed
  docker rm -f "$name" > /dev/null 2>&1 || true
  docker run -d --name "$name" -p 127.0.0.1:9083:9083 -p 127.0.0.1:9088:9088 -v "$PC/shop/build/install/shop-n:/app:ro" \
    -e HTTP_PORT=9083 -e METRICS_PORT=9088 -e INSTANCE_NAME=app-skewed \
    -e POSTGRES_URL=jdbc:postgresql://host.docker.internal:55432/outbox \
    -e TARGET_DB_URL=jdbc:postgresql://host.docker.internal:55433/outbox -e TARGET_URL=http://host.docker.internal:9090 \
    -e LD_PRELOAD=/usr/lib/aarch64-linux-gnu/faketime/libfaketimeMT.so.1 -e FAKETIME=+2h -e FAKETIME_DONT_FAKE_MONOTONIC=1 \
    -e JAVA_OPTS=-Xmx768m kora-outbox-prodcheck-app /app/bin/shop > /dev/null
  wait_for 90 curl -sf localhost:9088/system/readiness
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
  docker stop -t 20 "$name" > /dev/null
  docker rm -f "$name" > /dev/null 2>&1 || true
  fault "экземпляр с часами ушёл, двое с верными часами; цель вернулась"
  tfault 'mode=ok'
  pc_finish --no-time-checks --timeout 60
}
