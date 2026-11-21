#!/usr/bin/env bash
# poison: order mode at RATE msg/s plus COUNT messages over Kafka's max.request.size; they must land in the dead table
# without slowing the rest or breaking per-key order.
source "$(dirname "$0")/lib.sh"
RATE=${RATE:-2000}
COUNT=${COUNT:-100}
table=bench_poison

log poison
fresh_app poison "$table" "${SINGLE_ENV[@]}"
id=$(mark poison "poison: $RATE msg/s + $COUNT oversized messages")
load "$RATE" "$WRITERS"
sleep 30
t0=$(now)
sleep 60
t1=$(now)
curl -sf -XPOST "$BENCH/poison?count=$COUNT" > /dev/null
t2=$(now)
sleep 90
t3=$(now)
load_stop
reconcile_timed "$table" 900 "$(metric bench_messages_sent_total)"
sleep 35 # the dead-size gauge comes from maintenance, every 30 s
mark_end "$id"
dead=$(psql_q "select count(*) from ${table}_dead")
dead_gauge=$(metric outbox_dead_size)
retry=$(psql_q "select count(*) from ${table}_retry")
end=$(totals)
app_stop

row() { # row PHASE T0 T1
  window "$2" "$3"
  echo "| $1 | $(hms "$2") – $(hms "$3") | $(f0 "$COMMITTED") | $(f0 "$DELIVERED") \
| $(f0 "$CONSUMED") | $(f3 "$P50") s | $(f3 "$P99") s |"
}

{
  cat << EOF

## poison

Режим заказов (1 сообщение на транзакцию, писателей $WRITERS), $RATE msg/s, обработчик Kafka, консьюмер с inbox;
$APP_RELAY. В $(utc "$t1") UTC \`POST /poison?count=$COUNT\` закоммитил $COUNT сообщений размером
\`max.request.size\` продюсера + 1 КБ (~1 МБ каждое, различные ключи, $((t2 - t1)) с на все). Kafka отвергает их на
стороне клиента как постоянные сбои.

| фаза | окно (UTC) | закоммичено msg/s | доставлено msg/s | потреблено msg/s | задержка p50 | задержка p99 |
|---|---|---|---|---|---|---|
EOF
  row до "$t0" "$t1"
  row после "$t2" "$t3"
  cat << EOF

dead-таблица: $dead строк (gauge \`outbox.dead.size\` $dead_gauge), retry-таблица: $retry строк. Консьюмер всё
выбрал $DRAIN после остановки писателей. Итого: $end. Сверка (SQL, после выборки): закоммиченных строк, не потреблённых
и не dead: $RECON (в строках есть $COUNT poison-сообщений, а в счётчике писателей их нет).

$(consumer_note)
EOF
} | out
verdict "$table" "$RC"
