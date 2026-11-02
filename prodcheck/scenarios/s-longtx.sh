# Long transactions: «Длинные транзакции не задерживают доставку».
s_long_tx() {
  pc_begin long-tx
  trio "$MIX"
  sleep 5
  curl -sf -XPOST "$(pub 1001)/tx/hold?ms=40000&topic=payments&key=held&tag=held" > /dev/null
  fault "транзакция с send открыта 40 с, ключ held"
  sleep 3
  # a writer of the same key waits for the holder's key lock, everyone else goes by
  curl -s --max-time 60 -XPOST "$(pub 1002)/tx/send?topic=payments&key=held&tag=held" > /dev/null &
  local follower=$!
  sleep 12
  docker exec -d "$PG" psql -U outbox -d outbox -Atq -c 'begin isolation level repeatable read; select count(*) from pc_sent; select pg_sleep(25); commit' > /dev/null
  fault "ещё читатель со старым снапшотом 25 с"
  sleep 30
  wait "$follower" || true
  pc_finish --max-dups 0 --slow-keys held --p99-max 3
}
