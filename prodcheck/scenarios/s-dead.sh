# Dead letters and the admin API under load: «Судьбу dead letter решает хук или человек», «Dead letters», «Админ-API и HTTP».
http_code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
AB=/kora-outbox/admin/v1
pending_now() { [ "$(psql_q "select count(*) from outbox_dead where status = 'pending'")" -gt 0 ]; }

s_admin() { # each admin operation over HTTP on a live instance while the others write and deliver
  pc_begin admin
  trio 'rate=100&writers=4&keys=30&topics=payments&tags=poison:25'
  sleep 20
  local u
  u=$(pub 1002)$AB
  # access
  check "$(passes is "$(http_code "$u/status")" = 401)" "без ключа: 401"
  check "$(passes is "$(http_code -H "X-Api-Key: $READER_KEY" "$u/status")" = 200)" "ключ чтения читает: 200"
  check "$(passes is "$(http_code -H "X-Api-Key: $READER_KEY" -H 'Content-Type: application/json' -d '{"ids":[1]}' "$u/dead-letters/discard")" = 403)" "ключ чтения не меняет: 403"
  check "$(passes is "$(http_code -H "X-Api-Key: $ADMIN_KEY" -H 'Sec-Fetch-Site: cross-site' -H 'Content-Type: application/json' -d '{"ids":[1]}' "$u/dead-letters/discard")" = 403)" "изменяющий вызов с другого сайта: 403"
  # input
  check "$(passes is "$(admin_code 1002 POST dead-letters/redrive '{"filter":{}}')" = 400)" "пустой фильтр: 400"
  check "$(passes is "$(admin_code 1002 POST dead-letters/redrive '{"ids":[1],"nope":1}')" = 400)" "поле не из контракта: 400"
  check "$(passes is "$(admin_code 1002 POST 'orphans/outbox_tick/restore?fromXid=0')" = 400)" "не сирота: 400"
  check "$(passes is "$(admin_code 1002 POST recovery/xid-regression)" = 409)" "регрессии нет: 409"
  # paging: every letter once
  local after="" ids=$LOGS/pc-$SCN-ids.txt page next total
  : > "$ids"
  while :; do
    page=$(admin_body 1002 GET "dead-letters?topic=payments&status=kept&limit=7${after:+&afterId=$after}")
    echo "$page" | j '"\n".join(str(x["id"]) + " " + x["messageId"] for x in d["items"])' >> "$ids"
    next=$(echo "$page" | j 'd.get("nextAfterId") or ""')
    [ -n "$next" ] || break
    after=$next
  done
  sed -i '' '/^$/d' "$ids"
  total=$(wc -l < "$ids" | tr -d ' ')
  check "$(passes gt "$total" 7)" "постранично прочитано писем: $total"
  check "$(passes is "$(sort -u "$ids" | wc -l | tr -d ' ')" = "$total")" "ни одно письмо не прочитано дважды"
  local counted
  counted=$(admin_body 1002 GET 'dead-letters/count?topic=payments&error=422' | j 'd["count"]' || echo 0)
  log "count by error substring: $counted"
  # discard by ids
  local d=$LOGS/pc-$SCN-discarded.txt body
  head -3 "$ids" | awk '{print $2}' > "$d"
  body="{\"ids\":[$(head -3 "$ids" | awk '{print $1}' | paste -sd, -)]}"
  check "$(passes is "$(admin_body 1002 POST dead-letters/discard "$body" | j 'd["count"]')" = 3)" "discard по ids: 3"
  # the target is fixed; redrive some by ids, the others by filter, under load
  tfault 'poisonFixed=true'
  body="{\"ids\":[$(sed -n 4,6p "$ids" | awk '{print $1}' | paste -sd, -)]}"
  # a letter whose key an open transaction is sending is skipped and not counted: «call again» (README)
  local redriven=0 n=0
  until [ "$redriven" = 3 ] || [ "$n" -gt 10 ]; do
    redriven=$((redriven + $(admin_body 1002 POST dead-letters/redrive "$body" | j 'd["count"]')))
    n=$((n + 1))
    sleep 1
  done
  check "$(passes is "$redriven" = 3)" "redrive по ids: 3 (вызовов $n)"
  sleep 3
  local i
  for i in 1001 1002 1003; do pc_load "$i" 'rate=100&writers=4&keys=30&topics=payments'; done
  redrive_until_empty 1002 '{"filter":{"topic":"payments","status":"kept"}}' 'topic=payments&status=kept'
  # a destructive call's line also names what it selected: by "admin-key" with count "3", ids "[...]": 200
  check "$(passes logs_have 'outbox admin POST .* by \"admin-key\"( with .*)?: 200')" "аудит-строка каждого вызова"
  check "$(passes logs_have 'denied: 401')" "аудит отказа"
  REPAIRED="POST dead-letters/redrive и discard на живом инстансе"
  # the refused first arrival of each letter and its redrive are two arrivals: no bound on duplicates here
  pc_finish --discarded-file "$d" --dead-tags poison --exempt-tags poison
}

s_hook() { # every decision of the hook, its failures and a kill -9 in the middle: no letter lost, each decided once
  pc_begin hook
  trio 'rate=100&writers=4&keys=30&topics=payments&tags=hook-keep:40,hook-discard:41,hook-redrive:42,hook-redrive-at:43' \
    -Dprodcheck.hook=policy -Doutbox.deadLetters.timeout=2s
  sleep 15
  local i mode
  for mode in throw hang bad-list; do
    for i in 1001 1002 1003; do curl -sf -XPUT "$(pub "$i")/hook?mode=$mode" > /dev/null; done
    fault "хук: $mode"
    sleep 7
    for i in 1001 1002 1003; do curl -sf -XPUT "$(pub "$i")/hook?mode=ok" > /dev/null; done
    sleep 2
  done
  check "$(passes gt "$(on 1001 metric outbox_dead_hook_failures_total)" 0)" "растёт outbox.dead.hook.failures"
  pc_kill 1003
  pc_shop 1003 -Dprodcheck.hook=policy -Doutbox.deadLetters.timeout=2s
  sleep 5
  for i in 1001 1002 1003; do pc_unload "$i"; done
  check "$(passes wait_for 60 sh -c "[ \"\$(docker exec $PG psql -U outbox -d outbox -Atq -c \"select count(*) from outbox_dead where status <> 'kept'\")\" = 0 ]")" "ни одного письма pending и scheduled не осталось: все решены"
  local kept discarded
  kept=$(psql_q "select count(*) from outbox_dead d join pc_sent s using (message_id) where s.tag in ('hook-redrive', 'hook-redrive-at') and d.redrives >= 1")
  check "$(passes gt "$kept" 0)" "переотправленные снова умерли и несут redrives ≥ 1: $kept"
  discarded=$(psql_q "select count(*) from outbox_dead d join pc_sent s using (message_id) where s.tag = 'hook-discard'")
  check "$(passes is "$discarded" = 0)" "решение discard удалило письма"
  # the cause is fixed: humans send the kept letters back
  tfault 'poisonFixed=true'
  redrive_until_empty 1001 '{"filter":{"topic":"payments","status":"kept"}}' 'topic=payments&status=kept'
  REPAIRED="хук сам; оставленные письма: POST dead-letters/redrive"
  pc_finish --discard-tags hook-discard --dead-tags hook-keep,hook-redrive,hook-redrive-at --exempt-tags hook-keep,hook-discard,hook-redrive,hook-redrive-at
}

s_hook_mixed_fleet() { # a fleet with and without the hook: letters wait for the hook while it lives, kept when it is gone
  pc_begin hook-mixed-fleet
  pc_target
  pc_shop 1001 -Dprodcheck.hook=policy -Doutbox.partitions.maintenanceInterval=1s
  pc_shop 1002 -Doutbox.partitions.maintenanceInterval=1s
  curl -sf -XPUT "$(pub 1001)/hook?mode=throw" > /dev/null
  # load on the hook's instance: a relay writes a letter by its own hook, so the one without it would write kept at once
  pc_load 1001 'rate=100&writers=2&keys=20&topics=payments&tags=hook-keep:20'
  check "$(passes wait_for 30 pending_now)" "пока жив инстанс с хуком, письма ждут его (pending)"
  sleep 5
  check "$(passes pending_now)" "инстанс без хука не забирает их у него"
  pc_kill 1001
  check "$(passes wait_for 40 sh -c "[ \"\$(docker exec $PG psql -U outbox -d outbox -Atq -c \"select count(*) from outbox_dead where status = 'pending'\")\" = 0 ]")" "инстанс с хуком умер, heartbeat истёк: письма стали kept"
  pc_finish --dead-tags hook-keep --exempt-tags hook-keep --allow-left dead-pending
}

s_dead_retention() { # kept letters older than retention go, pending ones never
  pc_begin dead-retention
  pc_target
  pc_shop 1001 -Dprodcheck.hook=policy -Doutbox.deadLetters.retention=1d -Doutbox.partitions.maintenanceInterval=1s
  curl -sf -XPUT "$(pub 1001)/hook?mode=throw" > /dev/null
  pc_load 1001 'rate=100&writers=2&keys=20&topics=payments&tags=hook-keep:10'
  sleep 8
  pc_unload 1001
  psql_q "update outbox_dead set failed_at = now() - interval '2 days'" > /dev/null
  sleep 5
  check "$(passes gt "$(psql_q "select count(*) from outbox_dead where status = 'pending'")" 0)" "старые письма pending не удаляются"
  curl -sf -XPUT "$(pub 1001)/hook?mode=ok" > /dev/null # now the hook keeps them; maintenance removes the old kept ones
  psql_q "update outbox_dead set failed_at = now() - interval '2 days'" > /dev/null
  sleep 5
  psql_q "update outbox_dead set failed_at = now() - interval '2 days'" > /dev/null
  check "$(passes wait_for 20 sh -c "[ \"\$(docker exec $PG psql -U outbox -d outbox -Atq -c 'select count(*) from outbox_dead')\" = 0 ]")" "оставленные письма старше retention удалены обслуживанием"
  pc_finish --discard-tags hook-keep --exempt-tags hook-keep
}
