---
name: operating-outbox
description: Use when monitoring, alerting on, sizing or troubleshooting dsudomoin/outbox in a running Kora or Spring Boot service, such as outbox.* metrics and dashboards, growing outbox.shard.lag, dead letters and the dead-letter hook, the admin API (OutboxAdmin, outbox-admin-kora, outbox-admin-spring-boot, /kora-outbox/admin/v1), redrive or discard, tail overflow, orphaned partitions, stranded shards, «outbox relay halted» or «xid regression detected», outbox.shards and the emergency reshard, «refuses this write», records waiting for another release (outbox.waiting.size, outbox.waiting.oldest, «wait for an instance that can handle them», abandoning them), OutboxIncompatibleContractException and outbox.messages.contract_rejected, OutboxMissingRouteException («has a local route here»), outbox.messages.route_rejected and route_unchecked, rolling out a new OutboxTopicHandler route (routes in /status), rolling deploys, upgrades and rollback to the previous release, pg_dump/restore or failover, Kafka delivery slow after RecordTooLargeException or an expired batch. Also «алерты outbox», «dead letters», «лаг аутбокса», «эксплуатация outbox», «админка outbox».
---

# Operating outbox

## Overview

Procedures: the library README (https://github.com/dsudomoin/outbox#readme). Act through the admin API: `OutboxAdmin` in
code or `outbox-admin-kora` / `outbox-admin-spring-boot` over HTTP ([admin-api.md](admin-api.md)). **Never improvise SQL
against the library's tables**; run only the README's SQL, on a staging copy first.

## Wake someone: delivery stops

| Signal | Meaning | README section |
|---|---|---|
| `outbox.shard.lag` growing | Target down, slow handler, too few workers; ERROR «outbox shards [...] start from ticks [...] that no longer exist»: stranded shards, one `POST /recovery/stranded-shards` | «Обработка сбоев», «Застрявшие шарды» |
| `outbox.maintenance.since_success` > 5 × `maintenanceInterval` | No maintenance anywhere; `transaction_pooling` = 1: a pooler | «Обслуживание работает там же, где relay» |
| `outbox.partitions.xids_left` not NaN | Tail detached: inserts fail at 0 | «Партиции» |
| `outbox.dead.pending.oldest` > X (X > `relay.retry.maxDelay`) and `outbox.dead.hook.failures` growing | Hook failing: letters wait | «Dead letters» |
| ERROR «outbox xid regression detected» | «recovered»: done; «relay halted»: its reason, maybe `POST /recovery/xid-regression` | «Восстановление после регрессии xid» |
| ERROR «... holds cursors for N shards, but this instance runs with outbox.shards = M ...» or «... refuses this write ...» | Old-count instance after a reshard: restart with the new count or unset | «Решардинг» |
| `outbox.waiting.oldest` > `outbox.relay.waiting.alertAfter` (15 min, rule `max(outbox_waiting_oldest) > 900`) or ERROR «outbox: N keys of ... wait for an instance that can handle them ...» | No running release reads them (half-rolled-out, or rolled back past the transition release): their keys stand, nothing is lost. N counts keys whose head waits; the later records of each key wait behind it. `outbox.messages.waiting{kind}` and the line name the cause (`contract`, `no_route`: also a record marked `outbox-local` for a route this release lacks; it never goes to Kafka). Deploy the release that reads them; for a final rollback, abandon them (below) | «Версии контрактов и откат» |
| `outbox.messages.contract_rejected` growing | A release writes a contract version a live relay instance cannot read: those business transactions fail with `OutboxIncompatibleContractException` (WARN «outbox ... refused a send» names the instances). Deploy the reading release everywhere first | «Версии контрактов и откат» |
| `outbox.messages.route_rejected` growing | A release writes to its route's topic while a live relay instance does not report that route: those business transactions fail with `OutboxMissingRouteException` (WARN «outbox ... refused a send» names the instances). Writing was switched on before the heartbeat showed the route everywhere, or a release without it still runs: deploy the route everywhere, then switch writing on. A refusal naming no instance («... has never read the live relay instances' reports ...», or «... has not read the live relay instances' reports for N s ...», the seconds only once a read happened): the writer's own heartbeat fails; check its connection source as for `route_unchecked` below | «Версии контрактов и откат» |

## Morning: nothing lost, waits for a human

| Signal | README section |
|---|---|
| `outbox.dead.size` grows, `outbox.dead.pending` does not | «Dead letters» |
| `outbox.partitions.orphaned` or `outbox.inbox.partitions.orphaned` > 0 | «Осиротевшие партиции» |
| `outbox.partitions.overflow` = 1 past an extension, `premade` < 2 | «Переполнение хвоста (tail overflow)» |
| ERROR «... has not run», «... over outbox.partitions.maxTailScan» | «Партиции» |
| `outbox.parked.size` not back to its level, or a key failing for ever with `maxAttempts = 0` | «Алерты»; clear one key now with `keys/abandon` ([admin-api.md](admin-api.md)) |
| `outbox.messages.route_unchecked` growing: a writer's heartbeat has not read the reports for over 3 × `maintenanceInterval` (WARN «outbox heartbeat of instance ... failed»), so the writer guard lets sends through by its last read. Nothing is lost (an unreadable record waits). The heartbeat's own pool (Kora default) does not depend on the app pool, so check the heartbeat source itself: WARN «outbox heartbeat connection source of ... failed, this beat uses the application's connections ...», ERROR «outbox heartbeat connection source reaches another database ...», the `hikaricp_*` meters with `pool="<db.poolName>-outbox-heartbeat"`, or `outbox.heartbeat.dedicatedConnection = false` (startup INFO «outbox heartbeat shares the application's connections ...»). A slow database or a transaction-mode pooler delays it even on its own pool | «Версии контрактов и откат» |

Meters and logs: [metrics.md](metrics.md); dashboard `observability/grafana/dashboards/outbox.json`.

## Never

- Change rows of `<table>_retry`, `_parked`, `_dead`, `_cursor`, `_instance` or `_halt` by hand; use the admin API.
- Detach outbox partitions by hand, or drop or truncate library tables while instances run.
- Disable the relay everywhere: maintenance and the dead-letter pass run only with it.

## Shards and pool

- **`outbox.shards`: leave it unset** (256 for a new outbox; an existing one keeps its count). A differing set value
  is the **emergency reshard**: stop writers, wait for `outbox.shard.lag` 0 and empty retry and parked tables (waiting
  records block it until delivered or abandoned), stop all instances (old release too), start
  one with the new value, then the rest; startup refuses while another instance with a different count runs
  («Решардинг», [config-reference.md](../using-outbox/config-reference.md)).
- **Pool:** `db.maxPoolSize` ≥ own peak + `relay.workers` + 3, + 1 while `drain()` runs. Profiles:
  [sizing-examples.md](../using-outbox/sizing-examples.md).
- **Heartbeat connection:** in Kora (and in Spring Boot, see below) the heartbeat takes nothing from `db.maxPoolSize`;
  it holds a pool of its own, `<db.poolName>-outbox-heartbeat`, one connection always open: +1 server connection per
  instance. Count it into `max_connections`, the role's `CONNECTION LIMIT` and a pooler's per-user pool. Where that
  limit is sized to exactly instances × `maxPoolSize`, raise it by 1 per instance BEFORE the upgrade, or set
  `outbox.heartbeat.dedicatedConnection = false`. Otherwise not every connection fits: Kora's app pools default to
  `minIdle = 0` and grow under load, while the heartbeat pool holds its connection from start. Either the heartbeat pool
  cannot connect, and the heartbeat runs on the app pool as before, with a WARN once a minute («... this beat uses the
  application's connections ...»); the heartbeat pool retries without end, at most 5 s apart, so with a role limit
  Postgres logs FATAL «too many connections for role …» about every 5 s per instance, which may fire log-based alerts.
  Or it connected first, and at peak an app pool cannot reach `maxPoolSize`, so a `getConnection` on it (business
  code's, the relay's, the admin's) waits up to its `connectionTimeout` and fails if no connection frees. The two can
  swap as idle app connections close and connections are replaced every `maxLifetime`, but the heartbeat pool takes the
  first slot freed within 5 s, while an app pool connects only when a connection is wanted, so it tends to settle on the
  second: the app pool one connection short at peak ([config-reference.md](../using-outbox/config-reference.md)).

## Spring Boot

Metrics, alerts and procedures are the same. What differs:

- **Pool:** `spring.datasource.hikari.maximum-pool-size` ≥ own peak + `relay.workers` + 3, + 1 while `drain()` runs, +
  one per concurrent `@Transactional` handler call. Hikari's default of 10 is below that at the default 4 workers with
  such handlers (11).
- **Heartbeat:** by default a copy of the app's Hikari pool, one connection always open, named `<pool>-outbox-heartbeat`
  (`outbox-heartbeat` while the app's pool has no name: set `spring.datasource.hikari.pool-name`). Count it into limits
  as above. `outbox.heartbeat.dedicated-connection=false` or a `DataSource` bean with `@OutboxHeartbeat`.
- **Stop:** the web server and the listeners stop first, then the outbox, synchronously, within
  `outbox.relay.shutdown-timeout`; `spring.lifecycle.timeout-per-shutdown-phase` does not cut it. Set
  `terminationGracePeriodSeconds` above `preStop` + that phase timeout + `shutdown-timeout`; the default 30 s is too
  little. SIGKILL loses nothing: unrecorded records are delivered again.
- **Secrets:** `/actuator/configprops` with `show-values=always` prints `outbox.kafka.driver-properties` values.
- **Spans:** Boot 4.1 needs `spring-boot-micrometer-tracing` and a bridge, Boot 3.5 a bridge.
- **Admin HTTP:** [admin-api.md](admin-api.md), «Spring Boot».

## Deploys and incidents

- «Rolling-деплои»: identical schema-qualified `outbox.table`, `outbox.inbox.table`, inbox `retention` and
  `partitionInterval`. An event's breaking change goes out in two releases (using-outbox, «Версии контрактов и
  откат»): rollback to the transition release waits for nothing; past it, the newer records wait.
- **New `OutboxTopicHandler` route**: ship it everywhere, check that `routes` in `/status` `instances` lists its topic
  for every live relay instance, then switch writing on (a flag); earlier writes fail with
  `OutboxMissingRouteException`, as intended. Rolled back past it, its marked records wait (`no_route`), never reach
  Kafka: roll forward or abandon them. Remove a route in two releases (stop writing, then drop it once drained).
  Rolling back to a release built with a library version that lacks `outbox-local` (5fc60dd) is unsupported once
  route records exist.
- **Final rollback past the transition release**: records nobody will read again wait forever. Dry-run, then abandon
  them per topic (or key) to `kept` dead letters, the whole key at once ([admin-api.md](admin-api.md)); redrive or
  discard them later. Later records of an abandoned key are delivered ahead of it.
- **Rollback to 5fc60dd** («Обновление с предыдущего релиза»): set its `outbox.shards` to the cursors' count (256 if
  this release created it); then list the window's `kept` dead letters.
- «Репликация, failover и PITR»: async failover may deliver events whose data is gone; PITR redelivers.
- `relay.retry.maxAttempts = 0` (default): a key stuck on a broken target shows as lag only; `keys/abandon` clears it without a rollout.
- Kafka records slower for up to 10 min after a call expired or was cut, often with a `RecordTooLargeException` dead
  letter («Требования к Kafka»): on kafka-clients ≤ 4.1 an oversized record shared a producer batch, and the sender
  resends its batch-mates unbatched. Nothing is lost; fix the cause with `batch.size` ≤ the topic's
  `max.message.bytes`, or kafka-clients 4.2+.
- Upgrades: Postgres 14+ (13 fails at startup), Kora 1.2.23+ (using-outbox).
- Writers' timeouts: ≥ 300 ms by day, ≥ `tailConversion.maxStall` + 300 ms at night.
