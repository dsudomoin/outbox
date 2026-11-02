# Rolling deploys, contract versions, routes, rollback: «Откат релиза не теряет записей», «Rolling-деплои», «Версии
# контрактов и откат».
TYPED='rate=100&writers=3&keys=20&topics=orders,payments'
waits() { [ "$(st 1001 'd["waitingSize"]')" -gt 0 ]; }

roll() { # roll BIN I...: replaces the instances one by one, each graceful, under their load
  local bin=$1 i
  shift
  for i in "$@"; do
    inst_stop "$i"
    fault "выкатка: экземпляр $i → $(basename "$(dirname "$(dirname "$bin")")")"
    pc_app "$bin" "$i"
    sleep 4
  done
}
beat() { sleep 7; } # a heartbeat of maintenanceInterval 5 s has passed

s_rolling_upgrade() { # N-1 → N → N+1 under writes: no refusal, no wait, key order holds
  pc_begin rolling-upgrade
  pc_target
  local i
  for i in 1001 1002 1003; do pc_app "$SHOP_N1" "$i"; pc_load "$i" "$TYPED"; done
  sleep 8
  roll "$SHOP" 1001 1002 1003
  beat
  roll "$SHOP_N2" 1001 1002 1003
  sleep 8
  check "$(passes is "$(st 1001 'd["waitingSize"]')" = 0)" "ни одна запись не ждёт"
  check "$(passes is "$(logs_count 'refused a send')" = 0)" "ни одного отказа при записи"
  check "$(passes gt "$(psql_q "select count(*) from pc_sent where contract = 2")" 0)" "N+1 действительно пишет v2"
  pc_finish --max-dups 0
}

s_skip_release() { # N+1 beside a live N-1: the first v2 send is refused before the write; heals when N-1 is gone
  pc_begin skip-release
  pc_target
  pc_app "$SHOP_N1" 1001
  pc_app "$SHOP_N1" 1002
  pc_load 1001 "$TYPED"
  pc_app "$SHOP_N2" 1003
  local out
  out=$(tx_send 1003 'topic=orders&key=z1&tag=normal')
  check "$(passes grep -q OutboxIncompatibleContractException <<< "$out")" "v2 рядом с живым N-1 отвергается при записи: ${out:0:160}"
  check "$(passes gt "$(on 1003 metric outbox_messages_contract_rejected_total)" 0)" "растёт outbox.messages.contract_rejected"
  check "$(passes logs_have 'refused a send')" "WARN «refused a send» называет инстансы"
  inst_stop 1001
  pc_app "$SHOP" 1001
  inst_stop 1002
  pc_app "$SHOP" 1002
  pc_load 1001 "$TYPED"
  local passed=1 n=0
  while [ "$n" -lt 30 ]; do
    if tx_send 1003 'topic=orders&key=z2&tag=normal' | grep -q '"id"'; then passed=0; break; fi
    n=$((n + 1))
    sleep 1
  done
  check "$passed" "когда N-1 нет, та же отправка проходит (через $n с)"
  REPAIRED="докатка релиза N на живые инстансы, без остановки всех"
  sleep 5
  pc_finish --max-dups 0
}

s_rollback_to_transition() { # N+1 → N: N reads what N+1 wrote, nothing waits
  pc_begin rollback-to-transition
  pc_target
  local i
  for i in 1001 1002; do pc_app "$SHOP_N2" "$i"; pc_load "$i" "$TYPED"; done
  sleep 10
  tfault 'mode=error'
  sleep 5
  roll "$SHOP" 1001 1002
  tfault 'mode=ok'
  sleep 10
  check "$(passes is "$(st 1001 'd["waitingSize"]')" = 0)" "откат на переходный релиз: ничего не ждёт"
  check "$(passes gt "$(target_q "select count(*) from pc_effect where contract = 2")" 0)" "N доставил записи v2"
  pc_finish
}

v2_backlog() { # N+1 writes v2 with the target down, a backlog of v2 stays behind; then the fleet rolls back to N-1
  pc_target
  local i
  for i in 1001 1002; do pc_app "$SHOP_N2" "$i"; pc_load "$i" "$TYPED"; done
  sleep 8
  tfault 'mode=error'
  sleep 8
  roll "$SHOP_N1" 1001 1002
  tfault 'mode=ok'
  check "$(passes wait_for 40 waits)" "откат за переходный релиз: записи v2 ждут (waitingSize > 0)"
  sleep 12
  local attempts
  attempts=$(psql_q "select coalesce(max(attempts), 0) from outbox_retry where waiting_since is not null")
  log "attempts of waiting heads: $attempts"
  check "$(passes is "$(psql_q 'select count(*) from outbox_dead')" = 0)" "в dead letters ничего не ушло"
  check "$(passes gt "$(on 1001 metric outbox_messages_waiting_total)" 0)" "растёт outbox.messages.waiting"
  check "$(passes effects_grow 5 payments)" "другой топик идёт дальше, пока записи v2 ждут"
}

s_rollback_past_transition() { # N+1 → N-1: v2 records wait; back to N: delivered in key order, no dead letter
  pc_begin rollback-past-transition
  v2_backlog
  roll "$SHOP" 1001 1002
  REPAIRED="докатка релиза N, который читает v2, без остановки всех"
  sleep 5
  pc_finish
}

s_rollback_for_good() { # the same, but the rollback is final: abandon by admin API, redrive once a reader returned
  pc_begin rollback-for-good
  v2_backlog
  local body records
  body=$(admin_body 1001 POST waiting/abandon '{"topic":"orders","reason":"v2 rolled back for good","dryRun":true}')
  records=$(echo "$body" | j 'd["records"]')
  check "$(passes gt "$records" 0)" "dryRun считает, ничего не перенося: $body"
  check "$(passes is "$(psql_q "select count(*) from outbox_dead where error like 'abandoned%'")" = 0)" "dryRun ничего не перенёс"
  # the writers stop first: their v1 records of the same keys wait behind the heads and would keep adding keys to abandon
  pc_unload 1001
  pc_unload 1002
  local n=0 keys=1
  until [ "$keys" = 0 ] || [ "$n" -gt 10 ]; do
    body=$(admin_body 1001 POST waiting/abandon '{"topic":"orders","reason":"v2 rolled back for good"}')
    keys=$(echo "$body" | j 'd.get("keys", 1)')
    n=$((n + 1))
    sleep 3
  done
  check "$(passes is "$(st 1001 'd["waitingSize"]')" = 0)" "после отказа ничего не ждёт"
  check "$(passes gt "$(psql_q "select count(*) from outbox_dead where error like 'abandoned%' and status = 'kept'")" 0)" "отданные записи лежат kept с ошибкой abandoned"
  check "$(passes is "$(admin_code 1001 POST waiting/abandon '{"topic":"orders","key":"k1","reason":"x","nope":1}')" = 400)" "поле не из контракта: 400"
  roll "$SHOP" 1001 1002
  sleep 6
  local left=1
  n=0
  until [ "$left" = 0 ] || [ "$n" -gt 20 ]; do
    admin 1001 POST dead-letters/redrive '{"filter":{"topic":"orders","error":"abandoned"}}' > /dev/null
    left=$(admin_body 1001 GET 'dead-letters/count?topic=orders&error=abandoned' | j 'd["count"]')
    n=$((n + 1))
    sleep 2
  done
  check "$(passes is "$left" = 0)" "все отданные письма переотправлены фильтром"
  REPAIRED="POST waiting/abandon, после возврата читателя POST dead-letters/redrive"
  pc_finish --exempt-topics orders
}

s_route_canary() { # a new route beside instances without it: its send refused, the rest flows; written after the rollout
  pc_begin route-canary
  pc_target
  pc_app "$SHOP_N1" 1001
  pc_app "$SHOP_N1" 1002
  pc_load 1001 "$TYPED"
  pc_app "$SHOP" 1003
  beat
  local out
  out=$(tx_send 1003 'topic=audit&key=a1&tag=normal')
  check "$(passes grep -q OutboxMissingRouteException <<< "$out")" "запись в новый маршрут рядом с релизом без него отвергается: ${out:0:160}"
  check "$(passes gt "$(on 1003 metric outbox_messages_route_rejected_total)" 0)" "растёт outbox.messages.route_rejected"
  out=$(tx_send 1001 'topic=audit&key=a1&tag=normal')
  check "$(passes grep -q IllegalArgumentException <<< "$out")" "релиз без маршрута и без обработчика за маршрутами: опечатка, IllegalArgumentException"
  roll "$SHOP" 1001 1002
  beat
  check "$(passes is "$(st 1001 'all("audit" in x["routes"] for x in d["instances"] if x["live"] and x["relay"])')" = True)" "routes в /status подтверждают маршрут у всех"
  out=$(tx_send 1003 'topic=audit&key=a1&tag=normal')
  check "$(passes grep -q '"id"' <<< "$out")" "после выкатки запись проходит: ${out:0:120}"
  pc_load 1001 'rate=100&writers=3&keys=20&topics=orders,payments,audit'
  sleep 8
  REPAIRED="докатка маршрута, без остановки всех"
  pc_finish --max-dups 0
}

s_route_rollback() { # records of a route survive a rollback to the release without it: they wait, then flow when it returns
  pc_begin route-rollback
  pc_target
  local i
  for i in 1001 1002; do pc_app "$SHOP" "$i"; pc_load "$i" 'rate=100&writers=3&keys=20&topics=audit,payments'; done
  sleep 8
  tfault 'failKeys=k1,k2,k3'
  sleep 6
  LOADS[1001]='rate=100&writers=3&keys=20&topics=payments'
  LOADS[1002]='rate=100&writers=3&keys=20&topics=payments'
  roll "$SHOP_N1" 1001 1002
  tfault 'failKeys='
  check "$(passes wait_for 40 waits)" "записи маршрута ждут релиз с маршрутом (no_route)"
  check "$(passes is "$(psql_q 'select count(*) from outbox_dead')" = 0)" "в dead letters ничего не ушло"
  check "$(passes effects_grow 5 payments)" "другой топик идёт дальше"
  roll "$SHOP" 1001 1002
  REPAIRED="докатка релиза с маршрутом, без остановки всех"
  pc_finish
}

s_library_rollback() { # the same app built against the previous library version: mixed fleet, full rollback, back again
  pc_begin library-rollback
  pc_target
  local old=$PC/shop/build/install/shop-n-${PREV_OUTBOX:-0.0.0-89ad8fd}/bin/shop i
  for i in 1001 1002 1003; do pc_shop "$i"; pc_load "$i" "$TYPED"; done
  sleep 8
  roll "$old" 1001
  sleep 10
  roll "$old" 1002 1003
  sleep 15
  roll "$SHOP" 1001
  sleep 10
  roll "$SHOP" 1002 1003
  sleep 8
  check "$(passes is "$(logs_count ' ERROR ')" = 0)" "ни одной строки ERROR в логах обеих версий"
  pc_finish --max-dups 0
}
