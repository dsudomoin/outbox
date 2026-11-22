# outbox configuration reference

HOCON path `outbox` (`OutboxConfig`); every key is optional except `outbox.kafka.driverProperties."bootstrap.servers"`
when `KafkaOutboxModule` is in the graph. Values are validated at startup, before any DDL; a key that does not exist
here is silently ignored. Without a framework the same keys are the methods of `OutboxConfig` and `KafkaOutboxConfig`,
with the same defaults: override the ones you change. In Spring Boot they are `application.yml` keys: see «Spring Boot»
at the end.

```hocon
outbox {
  table = "app.outbox"               # the schema of the service's own tables, identical on every instance
  kafka.driverProperties {
    "bootstrap.servers" = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
  }
}
```

## `outbox`

| Key | Default | Rule and meaning |
|---|---|---|
| `table` | `"outbox"` | `[schema.]name`, each part `[A-Za-z_][A-Za-z0-9_]{0,40}`, lowercased. Base name of `<table>`, `_retry`, `_parked`, `_dead`, `_redriven`, `_tick`, `_checkpoint`, `_cursor`, `_halt`, `_maintenance`, `_stranded`, `_dropping`, `_instance`. Unqualified = `current_schema()` of each connection. The schema, or an unqualified name, must not be a reserved Postgres keyword (`user`, `order`, `left`...): names go into SQL unquoted, so start and `OutboxSql` both fail asking for a rename; `public.user` works. |

## `outbox.relay`

| Key | Default | Rule and meaning |
|---|---|---|
| `enabled` | `true` | `false`: no relay workers, no partition maintenance and no dead-letter pass on this instance (the hook then gets letters only from the pass at the end of `drain()`); `OutboxRelay.drain()` still works; the heartbeat still runs, and so does the ticker (thread `outbox-ticker`): while this instance writes it makes a tick once a second, so a backlog written while no relay runs comes in batches. It does nothing unless this instance committed since its last tick, inserts none when any instance's tick is at most 3/4 of a second old, and stops while an xid-regression halt is recorded. Needs the grants of `OutboxSql.outbox(config, role)`; a failure is a WARN at most once a minute. Ticks are deleted by maintenance, which runs only with a relay: with every relay down while someone writes the tick table grows by about one row a second (~26 MB a day). Keep it on for at least one production instance. |
| `workers` | `4` | ≥ 1. Relay threads per instance; each holds a DB connection while working, and under a backlog hands it back to the pool and takes another every minute, once the step in flight ends. |
| `batchSize` | `10000` | ≥ 1. Rows per chunk read and per handler call, for every handler. A call may also carry the parked (requeued) records of the chunk's keys: up to 2 × `batchSize` records. A key with more parked records than `batchSize` brings none of them: it comes in portions, see `requeue.md`. A handler spending milliseconds per record (sending records one by one) sets 1000 or lower: `batchSize` × time per record must stay well below 80 % of `batchTimeout` (10 ms × 10 000 = 100 s of the default 104), and a failed step redelivers every record of its call. At 10 000, rows above about 420 B (payload + key + headers) go through the byte-budgeted read (see `batchBytes`). A shard's read sorts about a thousand rows, so rows up to a few KB fit the default `work_mem` (4 MB; measured at 1 and 3 KB: no temp files). Measured against 1000 (Postgres 1 ms away, 4 workers, 256 shards): 300 B rows about 2.4× faster, 1 KB in the database's memory about 1.2×, a backlog larger than its memory within 10 %. |
| `batchBytes` | `16777216` | ≥ 0, 0 = no limit. Bytes (payload + key + headers as stored) per chunk read and per handler call, as `batchSize` bounds rows: a call takes records while they fit, at least one, so a record larger than this goes alone. Parked records and the retry pass get as much again (up to 2 × `batchBytes` a call); a key whose parked records pass `batchBytes` does not fit in a call. Heap for the relay: about `workers × 2 × batchBytes × 2` (the driver keeps a copy). While a shard's rows are small (`batchSize` of its largest recent rows below a quarter of `batchBytes`: up to about 420 B at the defaults), its chunk is read by the plain statement at no extra cost; larger rows go by the byte-budgeted one (at 1 KB still faster than `batchSize` 1000: 240k vs 200k msg/s in memory); the first read of a shard after a start or a worker's fatal error is always bounded, and a sudden jump in message size can make one unbounded read per worker. |
| `batchTimeout` | `130s` | > 0, ≤ 24 days. Bound of one handler call: wind-down at 80 %, abandoned (all records retried) at 100 %. With Kafka ≥ `delivery.timeout.ms + 5 s + max.block.ms`; the default is that budget with the default producer settings, a handler without such long calls may set it lower. `drain()` gives up after 4 × this + 10 s (530 s by default) without progress, longer than a relay step runs: a step over several shards starts a shard's retry pass until it has spent `batchTimeout`, one started just before runs up to 2 × `batchTimeout`, and the main pass's shared call up to another `batchTimeout`. |
| `shutdownTimeout` | `30s` | ≥ 0. Grace for in-flight batches at shutdown; then they are interrupted and redelivered later, a step still waiting for a Kafka call through an outage included. `close()` spends up to about 8 s more: interrupted workers and the instance row's delete (a locked row or an exhausted pool is left to expire). |

## `outbox.relay.retry`

| Key | Default | Rule and meaning |
|---|---|---|
| `initialDelay` | `1s` | > 0. First backoff of a failed key, of a paused shard and of a shard whose step fails alone a second time in a row; doubles per failure. |
| `maxDelay` | `5m` | ≥ `initialDelay`. Backoff ceiling. With Kafka keep it under 10 min: a record kept for an unbatched resend is forgotten after 10 min, and a later retry goes back into a shared batch (below, «Oversized records»). |
| `maxAttempts` | `0` | ≥ 0. After this many failed attempts a record moves to `<table>_dead`. `0` = retry forever. `NotAttempted` does not count. A single-key divert charges the head the shard's paused attempts only when the failures began at it; a head a probe reads after a divert gets its one attempt. While the target is down each probed row spends its own attempts over their backoff and goes dead. Lowering it moves records that already carry that many attempts to dead when next due, without another call; waiting heads are exempt. Global, Kafka included: a dead-lettered record lets the later records of its key go on, so with ordered topics keep 0 and bound a slow route in its handler (`record.attempts()`, writing-outbox-handlers). |
| `holdPermanent` | `false` | `true`: every `Permanent` a handler returns counts as a `Retry` with the same error, so the record becomes the retry head of its key instead of going to `<table>_dead`, and the later records of the key wait behind it. Attempts count, backoff grows to `maxDelay`, each handler call that held a record logs an ERROR, and `outbox.retry.oldest` is the alert. A key never skips its record only with `maxAttempts = 0`. A `handleBatch` of your own answers `NotAttempted` for the rest of a key after its `Permanent`; the Kafka sender then sends with one request in flight per broker connection, from producers of its own, so a large call is slower (a 10 000-record call took about 1.7× as long against a local broker). Resolve by fixing the cause (e.g. the topic's `max.message.bytes`) or with `abandonKey`. Spring: `outbox.relay.retry.hold-permanent`. |

## `outbox.relay.requeue`

Records a `handleBatch` override set aside with `Outcome.requeue()` (writing-outbox-handlers, `requeue.md`). A requeued record comes back after 1 s unless its key fits in a call (`batchSize` parked records and `batchBytes` at most) and a new record of the key, its retry head, or a newer parked record whose time has come, brings it back first, so the delay does not cut the repeats of a busy key (bound them with `maxRequeues` or the handler); `requeue(notBefore)` due not before `notBefore` (capped at 24 h). A parked record handed back and not attempted waits at least 1 s too.

| Key | Default | Rule and meaning |
|---|---|---|
| `maxRequeues` | `0` | ≥ 0. The requeue past this moves the record to `<table>_dead` with «requeue limit reached», attempts unchanged. Every hand-back answered with `requeue` counts, merges ahead of a new record of the key and early offers ahead of a newer due record of the key included. `0` = unlimited. Global; a handler can bound per route with `record.requeues()` (merges spend requeues at an unpredictable rate). Bound the count, not the age: `record.createdAt()` is the database's time and `Instant.now()` the JVM's, so the two differ by the clock offset. |

## `outbox.partitions`

Spring Boot: `outbox.partitions.tail-conversion.window`, `.zone`, `.max-stall`.

| Key | Default | Rule and meaning |
|---|---|---|
| `tailConversion.window` | `"03:00-05:00"` | `HH:mm-HH:mm` in `tailConversion.zone`, may cross midnight (`"23:00-01:00"`); `""`: never. Validated and logged at startup. Inside it maintenance also converts a hot tail over 16 MiB; an attempt cut by `maxStall` is not retried in that window, and one ERROR fires when the window ends without a fit. Tests: `""` (the fixture's default). |
| `tailConversion.zone` | JVM default zone | IANA zone id (`Europe/Moscow`), validated at startup. Containers usually run in UTC: set it. |
| `tailConversion.maxStall` | `1s` | 1ms..24 days. The window's hold bound (its statement's `statement_timeout`), so the writers' longest wait. At night writers' own `lock_timeout` and `statement_timeout` must each be ≥ `maxStall` + 300 ms (Postgres default `lock_timeout` 0 = none); else lower it. A scan that does not fit rolls back cleanly with a WARN. |

## `outbox.recovery`

What to do when the xid counter is found below a tick's xmax (README «Восстановление после регрессии xid (xid
regression recovery)»). The automatic recovery re-sends at most 1 000 000 rows; more leave the relay halted until
`recoverXidRegression` (admin API).

| Key | Default | Rule and meaning |
|---|---|---|
| `timeout` | `10m` | > 0, ≤ 24 days. Deadline of the whole recovery transaction (writers wait for it); each statement gets what is left; also the row count's `statement_timeout`. An automatic recovery that failed after locking the outbox is not tried again by itself. |

## `outbox.inbox` (`InboxModule`)

| Key | Default | Rule and meaning |
|---|---|---|
| `table` | `"inbox"` | Same naming rules as `outbox.table`. Created only when a component uses `Inbox`. |
| `retention` | `24h` | ≥ 2 h (a whole number of minutes); `retention + 24 h` ≤ 1000 h. Ids younger than `retention − 1 h` (23 h) are always deduplicated while inbox maintenance makes ranges ahead (`outbox.inbox.window.remaining`); a forward step of X on the database clock lowers that age by X, so leave the margin. Change on all instances at once; services sharing one `outbox.inbox.table` keep the same `retention`, or the shortest `retention` wins for all. |

An inbox-only service also reads `outbox.relay.shutdownTimeout`.

## `outbox.deadLetters`

The dead-letter hook and the dead table (writing-outbox-handlers, `dead-letter-hook.md`).

| Key | Default | Rule and meaning |
|---|---|---|
| `retention` | `0` | 0..3650 days. Maintenance deletes `kept` letters older than this by `failed_at`; `0` keeps them forever. `pending` and `scheduled` letters are never deleted. Deleted in portions, at most 30 s per run; later runs finish the rest. |

## `outbox.schema`

| Key | Default | Rule and meaning |
|---|---|---|
| `bootstrap` | `true` | `true`: every instance creates the missing base tables at startup and upgrades older ones. `false`: the application's migrations own them: render the SQL once with `OutboxSql.outbox(config, role)` (and `OutboxSql.inbox` for the inbox) into a Flyway or Liquibase migration; startup then creates nothing and checks every table, column (default expressions included, as `pg_get_expr` prints them), index, partition key and the roles' rights, failing with every problem and the schema version that adds a missing object. Partitions stay the library's in both modes, so its role owns the partitioned tables. A newer schema's extra columns, indexes and tables are accepted (rolling deploys). |

**DML-only application role.** With `bootstrap = false` the runtime DDL can run as the tables' owner while the app's
own role has DML only: `OutboxRuntime.builder(...).ownerConnections(ownerDataSource::getConnection)` (and the same on
`InboxRuntime`), or, with Kora, a second `JdbcDatabase` tagged `@Tag(OutboxOwner.class)`, or, in Spring Boot, a
`DataSource` bean with `@OutboxOwner`. Through the owner go startup,
maintenance (partitions, orphans, xid-regression recovery), `OutboxAdmin.status()`, `recoverStrandedShards`, `restoreOrphan` and
`recoverXidRegression`; through the app's role `send`, `tryAccept`, relay steps, the dead-letter pass, the other admin
calls. An owner source with `bootstrap = true` fails at build; one reaching another database fails startup before
any DDL (one database where an unqualified name finds another table, mostly the app role lacking `USAGE` on the
schema, gets its own message with the `GRANT`). Render the grants with `OutboxSql.outbox(config, "app_role")`; the role also needs `USAGE` on the schema, and
must not have `CREATE` on any schema of the owner's `search_path` (startup WARNs with the `REVOKE`). Every partition
is given to the parent table's owner: an owner source that is only a member of that role needs the `SET` option on
PG 16+, and the owner role needs `CREATE` on the schema; otherwise the partition stays the creating role's and a
WARN, at most hourly, names the `GRANT`. Partitions a member made before the handoff existed are given to the owner
by that member's next start or maintenance run. For a retired member, run this in `psql` as it (or a superuser),
with `<base>` and `<inbox>` the values of `outbox.table` and `outbox.inbox.table`: it builds one
`ALTER TABLE ... OWNER TO` per partition of either parent (found through `pg_inherits`) and per standalone tail,
`<base>_tail_next` or detached range (found by name), and `\gexec` runs them. Not `REASSIGN OWNED BY`: that moves
everything the role owns in the database, the app's own tables and sequences included.

```sql
select format('alter table %s owner to %I;', c.oid::regclass, '<owner>')
from (values ('<base>', '_(tail|tail_next|[xo][0-9]+)'), ('<inbox>', '_t[0-9]{10}([0-9]{2})?')) v (parent, suffix)
join pg_class p on p.oid = to_regclass(v.parent)
join pg_class c on c.oid in (select inhrelid from pg_inherits where inhparent = p.oid)
    or c.relnamespace = p.relnamespace and c.relname ~ ('^' || p.relname || v.suffix || '$')
where c.relowner = '<member>'::regrole \gexec
```

## Shard count

- A new outbox gets 256 shards; an existing one keeps the count of its cursors (INFO at startup). A shard is processed
  by one worker at a time across all instances; a key always maps to one shard. Many shards are cheap: idle shards
  write nothing ([sizing-examples.md](sizing-examples.md)).
- Every instance heartbeats into `<table>_instance` (id `<host>/<pid>/<8 hex>`, shard count, whether its dead-letter
  hook is active) every 30 s; `OutboxAdmin.status()` lists them as `instances`.

## `outbox.heartbeat` (`OutboxModule`)

In Kora as below; in Spring Boot `outbox.heartbeat.dedicated-connection` and `@OutboxHeartbeat` («Spring Boot» at the
end); without a framework, `OutboxRuntime.builder(...).heartbeatConnections(heartbeatPool::getConnection)`.

| Key | Default | Rule and meaning |
|---|---|---|
| `dedicatedConnection` | `true` | `true`: the heartbeat gets a pool of its own, so business load that exhausts the app pool does not starve its beats. A copy of the HikariCP pool of the `JdbcDatabase` behind the outbox's transactions (same database, role, driver properties and meters), named `<db.poolName>-outbox-heartbeat`, with `maximumPoolSize` and `minimumIdle` 1, `initializationFailTimeout` -1, `connectionTimeout` = max(500 ms, min(copied, 7.5 s)) and `validationTimeout` = max(250 ms, min(copied, `connectionTimeout` / 2)); closed after the runtime. Its connection stays open: +1 server connection per instance, outside `db.maxPoolSize`. `false`: the heartbeat takes the app's connections, as before. |

- A `JdbcConnectionFactory` tagged `@Tag(OutboxHeartbeat.class)` replaces the pool whatever the key says: to bypass a
  pooler, run as another role (with the app role's DML rights on the outbox tables and `USAGE` on their schema) or use
  other timeouts. It must reach the app's database; keep its connection and validation timeouts together well below
  30 s, as one borrow can take both.
- Transactions that are not Kora's over a Hikari-backed `JdbcDatabase` (an app's own `OutboxTransactions` or
  `JdbcConnectionFactory`) leave nothing to copy: the heartbeat shares the app's connections, and a startup INFO names
  the tag.
- It helps against an exhausted app pool only: not behind a transaction-mode pooler (its transactions still wait for
  the pooler's server connections) and not against a slow database. The username and password are copied once: a
  password set at runtime on the app's pool is not followed (Kora's `JdbcDatabase` reads it from config at start, so
  only if you change it on the running pool yourself), and once the copy's connection is replaced and the old one
  fails, beats fall back to the app pool with the WARN below. A `HikariCredentialsProvider` set on the app's pool is
  shared by the copy, so its credentials are followed.
- A role `CONNECTION LIMIT` or a pooler's per-user pool sized to exactly instances × `maxPoolSize`: raise it by 1 per
  instance before the upgrade, or set the key to `false`. Otherwise not every connection fits (Kora's app pools
  default to `minIdle = 0` and grow under load; this pool holds its connection from start):
  - this pool cannot connect: startup does not fail, and the heartbeat runs on the app pool as before, with a WARN once
    a minute («... this beat uses the application's connections ...»). Hikari retries this pool's connection without
    end, at most 5 s apart, so with a role limit Postgres logs FATAL «too many connections for role …» about every
    5 s per instance, which may fire log-based alerts;
  - it connected first: at peak an app pool cannot reach `maxPoolSize`, and a `getConnection` on it (business code's,
    the relay's, the admin's) waits up to its `connectionTimeout` and fails if no connection frees.

  The two can swap as idle app connections close (`idleTimeout`) and connections are replaced every `maxLifetime`.
  This pool takes the first slot freed within 5 s, while an app pool connects only when a connection is wanted, so
  the outcome tends to settle on the second: the app pool one connection short at peak.

## `outbox.admin` (`OutboxAdminHttpModule`)

In Kora as below; in Spring Boot `outbox.admin.check-origin`.

| Key | Default | Rule and meaning |
|---|---|---|
| `checkOrigin` | `true` | `true`: a call other than `GET`/`HEAD` without `Sec-Fetch-Site` gets 403 «cross-site request refused» when its `Origin` is `null` or another host:port than the request's `Host` (the scheme aside). `false`: `Origin` is not looked at, for browsers on plain HTTP behind a proxy that rewrites `Host` and cannot pass the browser's on. `Sec-Fetch-Site` is checked either way. |

## `outbox.kafka` (`KafkaOutboxModule`)

`deadLetterTopic` (default unset: Kafka DLQ off, letters stay `kept` in the dead table): topic template, `{topic}` is
the letter's topic, e.g. `"{topic}.dlq"`. Validated at startup: a Kafka topic name (up to 249 of `[a-zA-Z0-9._-]`),
not `{topic}` alone. Behaviour and duplicates: writing-outbox-handlers, `dead-letter-hook.md`.


`driverProperties`: Kafka producer properties, validated at startup.

| | Properties |
|---|---|
| Forced | `enable.idempotence=true`, `acks=all`, byte-array key and value serializers |
| Rejected | `transactional.id`; `partitioner.class`; `partitioner.ignore.keys=true`; `acks` other than `all`/`-1`; `enable.idempotence=false`; `max.in.flight.requests.per.connection` > 5; `delivery.timeout.ms` < `linger.ms + request.timeout.ms`; anything Kafka's `ProducerConfig` rejects |
| Defaults when unset | `delivery.timeout.ms=120000`; `max.block.ms=5000`; `enable.metrics.push=false`; then, if `delivery.timeout.ms` was unset too, `request.timeout.ms=15000` (half Kafka's own, so a request to a frozen broker moves on sooner) |

**Outcomes.** `RecordTooLargeException`, `InvalidRecordException` (a null key on a compacted topic among them) and
`InvalidTopicException` are permanent (dead letters); an `InvalidTopicException` sends every record of its topic in
the call to dead letters at once. Everything else, authorization errors included, retries.

**Oversized records.** The broker refuses a whole producer batch that holds a record over the topic's
`max.message.bytes`. kafka-clients 4.2+ split it until that record is alone and refused: permanent. kafka-clients
≤ 4.1 rebuild the same batch until `delivery.timeout.ms`, and every record in it times out. The sender handles both:
records still unacked when their batch expired or the relay cut the call (80 % of `batchTimeout`) are remembered by
message id (up to 10 000, each until a call delivers or refuses it, at most 10 min), and a call holding one goes
through a second producer with `batch.size=0`, where the oversized record is refused alone and its key goes on. What
remains on ≤ 4.1: the first call waits for the expiry or the cut, and calls with remembered records are slower
(about 0.2 s per 1 000 records instead of 0.02 s on a local stand). Keep `batch.size` ≤ the topic's
`max.message.bytes` and `relay.retry.maxDelay` < 10 min.

Producer client metrics are not bound to Micrometer; they are visible through JMX only. A set `client.id` is a
prefix, since JMX tells producers apart by it: each producer gets `<client.id>-outbox-<n>`, one of the second pool
`<client.id>-outbox-unbatched-<n>`, and those of calls under `relay.retry.holdPermanent` `<client.id>-outbox-held-<n>`
and `<client.id>-outbox-held-unbatched-<n>`. Broker quotas match the whole `client.id`, so a quota on the bare id does not
cover the outbox producers: set it on the user principal. With no `client.id` Kafka names them
`producer-<n>`.

## Spring Boot

The keys above, the same defaults, under `outbox.*` in `application.yml`. Canonical form is kebab-case
(`outbox.relay.batch-timeout`, `outbox.kafka.driver-properties`, `outbox.kafka.dead-letter-topic`); camelCase binds too.
The core's validation messages name keys in camelCase. Durations take Spring's formats: `30s`, `1h`, `PT30S`, a bare
number is milliseconds; HOCON's `30 seconds` fails the start naming the key. Producer properties bind with their dots,
with or without brackets: `outbox.kafka.driver-properties.max.block.ms=3000`. The adapter does not read
`spring.kafka.*`: write `bootstrap.servers: ${spring.kafka.bootstrap-servers}`.

| Spring-only key | Default | Meaning |
|---|---|---|
| `outbox.enabled` | `true` | `false`: no `Outbox`, `OutboxAdmin`, relay or admin API; `Inbox` stays |
| `outbox.json.mapper` | `auto` | `auto`: Jackson 2's `ObjectMapper` bean if any, else Jackson 3's `JsonMapper`; `jackson2`, `jackson3`: that one, the start failing when absent |
| `outbox.kafka.fallback` | `true` | `true`: Kafka takes every topic no route claims, and an app's own `OutboxHandler` bean fails the start. `false`: that bean takes them or, with none, a send to such a topic is refused before writing |

The `SmartLifecycle` phase is fixed at `2147479551` (`Integer.MAX_VALUE - 4096`): below the web server, the Kafka
listener containers and the default phase, so the outbox starts before them and stops after. Observing the delivery of
a record whose stored trace is not sampled is off; to turn it on, define your own `OutboxTelemetry` bean with
`ObservationOutboxTelemetry.builder().observeUnsampled(true)`.

`outbox.enabled` and `outbox.kafka.fallback` take every boolean Spring binds: `false`, `off`, `no`, `0` turn them off,
`true`, `on`, `yes`, `1` on, in any case; any other value fails the start.

`outbox.admin.check-origin` is `checkOrigin` above, compared with the server name and port the container took
(from `Host`, or from proxy headers under `server.forward-headers-strategy`).

`outbox.heartbeat.dedicated-connection` is `dedicatedConnection` above: a copy of the `HikariDataSource` behind the
outbox, named `<pool>-outbox-heartbeat`, or `outbox-heartbeat` while the app's pool has no name, so set
`spring.datasource.hikari.pool-name`. A `DataSource` bean with `@OutboxHeartbeat` replaces it whatever the key says. An
app's own `OutboxConfig` bean replaces the core's settings; the keys of the table and `outbox.heartbeat.*` are still
read.
