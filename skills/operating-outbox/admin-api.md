# outbox admin API

What needs a human decision is one call, not hand SQL (README «Админ-API и HTTP»). Any instance answers the same:
they all work on the shared database.

## In code: `OutboxAdmin`

A component of `OutboxModule`, a bean in Spring Boot. Each call runs its own transactions; inside `inTx` it throws
`IllegalStateException`.

| Method | Does |
|---|---|
| `status()` | Relay halt, lag, retry and parked sizes, waiting records (`waitingSize` heads, `waitingOldestSeconds` for the oldest, 0 when none), dead letters by topic and status, orphans, tail overflow, `premade`, `xidsLeft`, seconds since maintenance, shards, and `instances`: every heartbeat row, expired ones included (id, shard count, `lastSeen`, whether its dead-letter hook is active, `relay`, `live`: its row has not expired, by the database clock, `contracts`: the highest contract version it reads by topic, null unknown, empty none, a non-positive value shown as 0, and `routes`: the topics its routes claim, typed and plain, sorted, null unknown (absent over HTTP), empty none). A new route may be written to once every instance with `relay` and `live` lists its topic in `routes`, and that set matches your deployment. `live` says the process runs, not that its queue moves. A `lastSeen` older than 3 × `maintenanceInterval` means the instance is not running; a crashed instance's row stays about an hour after its heartbeat expires, 3 × `maintenanceInterval` + 1 h after its last beat. |
| `findDeadLetters(filter, afterId, limit)`, `countDeadLetters(filter)` | Dead letters by `DeadLetterFilter` (topic, key, error substring, `failedFrom` inclusive, `failedTo` exclusive, status), id order, up to 1 000 per page. |
| `redriveDeadLetters(ids)` / `(filter)` | Back to the outbox as the hook's `redrive`: a new message after the later messages of its key, same message id. Returns the count. |
| `discardDeadLetters(ids)` / `(filter)` | Delete; returns the count. |
| `restoreOrphan(table, fromXid)` | Rows at `fromXid` and above back to the outbox, the rest dropped with the table. |
| `recoverXidRegression()` | The xid regression recovery now, in any mode and for any `maxRows`. |
| `recoverStrandedShards()` | Repairs shards whose starting tick, or their batch's target tick, no longer exists, on running instances; returns the rows re-sent. Refused while an xid regression halt is recorded. |
| `abandonWaiting(topic, key, reason, dryRun)` | Records of `topic` (of `key` only unless null) waiting for an instance able to handle them, the whole key each (waiting head, its followers, its parked records; an unkeyed head alone), to the dead table as `kept` (the hook is not asked) with error `abandoned: <reason>`. Returns `Abandoned(keys, records)`; `dryRun` counts only. Shard by shard, chunks of 1 000 keys, each under the shard's lock, waited up to `batchTimeout` + 5 s, then `IllegalStateException` «... busy, retry» (done chunks stay done; call again). Later records of the key are delivered ahead of the abandoned ones. |
| `abandonKey(topic, key, reason, dryRun)` | Every row of `key` in the retry and parked tables, whether or not its head waits, to the dead table as `kept` (the hook is not asked) with error `abandoned: <reason>`. For a record that requeues for ever or fails for ever with `maxAttempts = 0`. Returns `Abandoned(keys, records)`, with `dryRun` what it would move. Records not read yet, or sent later, are delivered: it moves only the rows there when its first chunk runs, and what the relay reads between chunks stays, the lowest of it made the head, due now. Chunks of 1 000 rows, newest first, under the shard's lock: «busy, retry» past `batchTimeout` + 5 s, calling again goes on. |
| `abandonRecord(topic, messageId, reason, dryRun)` | The same for an unkeyed record, by message id; a record with a key is refused. |

- Bulk calls take letters dead when the call starts, in chunks of 1 000 by id, one transaction per chunk. Letters
  held by a running hook call, and (redrive) letters whose key an open transaction sends, are skipped without
  waiting and not counted: call again, then check the count with the same filter. An empty filter is rejected.
- Every changing call logs one INFO line with its arguments and result; ids as their count and the first ten.
- Over HTTP, both adapters' audit line (`by "<identity>" with ...: <status>`) names what a destructive call selected:
  redrive and discard the filter's six fields or `count` and the first ten `ids`, orphan restore `fromXid`.

## Over HTTP: `outbox-admin-kora`

`OutboxAdminHttpModule` serves the same calls on the service's public HTTP server under `/kora-outbox/admin/v1`
(OpenAPI 3 contract `openapi/kora-outbox-admin.yaml` in the jar). It needs Kora's HTTP server, a JSON module and
exactly one `OutboxAdminAuthorizer` component: without one the service does not build. Deny with
`OutboxAdminAuthorizer.unauthorized` (401) or `forbidden` (403), never by throwing (any exception is a 500
«authorization failed»). Expose it only on an internal network or behind TLS: answers carry payloads. The
`X-Api-Key` header below is the README's example authorizer; yours may differ.

```bash
API=http://app:8080/kora-outbox/admin/v1
KEY="X-Api-Key: $OUTBOX_ADMIN_KEY"
JSON="Content-Type: application/json"

curl -s -H "$KEY" $API/status                                                  # first thing in the morning
curl -s -H "$KEY" "$API/dead-letters?topic=orders&status=kept&limit=100"       # next page: &afterId=<nextAfterId>
curl -s -H "$KEY" "$API/dead-letters/count?topic=orders&error=timeout"
curl -s -H "$KEY" -H "$JSON" -d '{"ids":[1001,1002]}' $API/dead-letters/redrive
curl -s -H "$KEY" -H "$JSON" -d '{"filter":{"topic":"orders","error":"timeout"}}' $API/dead-letters/redrive
curl -s -H "$KEY" -H "$JSON" -d '{"ids":[1003]}' $API/dead-letters/discard
curl -s -X POST -H "$KEY" "$API/orphans/outbox_x83886080/restore?fromXid=83900000"
curl -s -X POST -H "$KEY" $API/recovery/stranded-shards                         # {"count": N}
curl -s -X POST -H "$KEY" $API/recovery/xid-regression
curl -s -H "$KEY" -H "$JSON" -d '{"topic":"orders","reason":"v2 rolled back for good","dryRun":true}' \
    $API/waiting/abandon                                                       # {"keys":3,"records":5}
curl -s -H "$KEY" -H "$JSON" -d '{"topic":"orders","key":"order-42","reason":"handler bug, TASK-1","dryRun":true}' \
    $API/keys/abandon                                                          # {"keys":1,"records":7}
```

- 200 with JSON (changing calls: `{"count": N}`; `waiting/abandon`, `keys/abandon`: `{"keys": N, "records": M}`); 400 rejected input
  (unknown or repeated field, so a mistyped `key` or a second `"key": null` never widens an abandon; over 1 000 `ids`, empty filter, not an orphan, blank
  `topic` or `reason`; in `keys/abandon` not exactly one of `key` and `messageId`, or the `messageId` of a keyed record, or a body with no `Content-Type`); 401 / 403 from the authorizer; 415 a body that is not `application/json`; 409 not now (maintenance running, or a relay step holding the
  shard past `batchTimeout` + 5 s: retry; no xid regression); 500 otherwise. Every error body is `{"message": "..."}`.
- Decide by a credential sent on purpose (a header), not a cookie. A non-GET/HEAD call gets 403 before the authorizer
  when `Sec-Fetch-Site` is other than `same-origin`/`none`, or, with no `Sec-Fetch-Site` (browsers send none on plain
  HTTP to a non-localhost address), when `Origin` is `null` or another host:port than the request's `Host` (the scheme
  aside). Kora reads no forwarded headers: a proxy in front must pass the browser's `Host` on (nginx
  `proxy_set_header Host $http_host`). curl and services send neither header.
- Reads (`/status`, find, count) wait at most 5 s for a lock and 60 s per statement: during an xid regression
  recovery, which holds its tables, `/status` answers 500 (55P03) after 5 s; retry once it is done.
- A body holds exactly one of `ids` (up to 1 000) and `filter`. `status` is `pending`, `kept` or `scheduled`. A filter
  key is UTF-8 text: for a binary key use `ids` or `OutboxAdmin`.
- Times come back in milliseconds while Postgres keeps microseconds: a `failedAt` copied into `failedFrom`/`failedTo`
  can miss its own letter. Use `ids` for an exact set.
- Adding the contract to `openapi.management.file` moves the service's own spec from `/openapi` to
  `/openapi/<file name>`: tell whoever downloads it.

## Spring Boot

`outbox-admin-spring-boot` serves the same paths and JSON on Spring MVC, port `server.port`; the calls and answers above
hold. Without MVC or with `outbox.enabled=false` there are no routes and no authorizer is needed. Otherwise the app
defines one `OutboxAdminAuthorizer` bean (of `io.github.dsudomoin.outbox.admin.spring`), or the start fails; of several,
one `@Primary`.

```java
@Component
class ApiKeyAuthorizer implements OutboxAdminAuthorizer {

    private final byte[] adminKey;

    ApiKeyAuthorizer(@Value("${outbox-admin.admin-key}") String adminKey) {
        if (adminKey.isBlank()) {
            throw new IllegalArgumentException("the outbox admin key must not be blank");
        }
        this.adminKey = adminKey.getBytes(UTF_8);
    }

    @Override
    public Decision authorize(HttpServletRequest request) {
        var header = request.getHeader("X-Api-Key");
        var key = header == null ? new byte[0] : header.getBytes(UTF_8);
        return MessageDigest.isEqual(key, adminKey) ? OutboxAdminAuthorizer.allow("admin-key")
                : OutboxAdminAuthorizer.unauthorized("X-Api-Key required");
    }
}
```

- It runs on the request's thread and may block; it must not read the body. Deny with `unauthorized` (401) or
  `forbidden` (403), never by throwing: any throw or null is a 500 «authorization failed».
- Decide by a credential sent on purpose (a header), not a cookie. A non-GET/HEAD call gets 403 before the authorizer
  when `Sec-Fetch-Site` is other than `same-origin`/`none`, or, with no `Sec-Fetch-Site` (browsers send none on plain
  HTTP to a non-localhost address), when `Origin` is `null` or another host:port than the server name and port the
  container took from `Host`, or from proxy headers under `server.forward-headers-strategy` (the scheme aside). curl
  and services send neither header.
- Audit: logger `io.github.dsudomoin.outbox.admin.spring.OutboxAdminGuard` at INFO; keep it on. An allowed call's line
  comes just after its response is flushed.
- Differs from Kora: a body over 1 MiB gets 400; blank `?limit=` and
  `?afterId=` read as absent; unmatched 404/405/406 and OPTIONS go to the app's default handling, no route run. A
  multipart request is parsed ahead of authorization on every path.
- Request bodies are read with the library's own mapper, so the app's Jackson settings and deserializers cannot change
  what a call means. Jackson 2 or 3 must be on the classpath, or the start fails.
- A 500 «authorization did not run»: something lost the guard (an app's `HandlerMapping` dropping interceptors, the
  controller scanned without the auto-configuration). It stays closed; the WARN names the cause.
- App settings that break it, all closed: `spring.aop.proxy-target-class=false` with an aspect over controllers;
  replaced exception resolvers; Gson as MVC's JSON converter (wrong status values, `key` and `payload` as number arrays
  in responses). A global
  `ResponseBodyAdvice` wraps its answers: scope it. Spring
  Security's CSRF applies to these POSTs: exempt the path or use a token.

## Procedures

- **Dead letters** («Dead letters»): `/status`, then list, redrive or discard. A redrive keeps the message id, so
  inbox consumers deduplicate it within `retention − partitionInterval` (23 h by default).
- **Orphans** («Осиротевшие партиции»): compare the orphan's rows with what consumers got (the README's `select`),
  then one `restore` call with the cut-off `fromXid` (`0` returns all).
- **xid halt** («Восстановление после регрессии xid (xid regression recovery)»): one `POST /recovery/xid-regression`;
  writers wait about 2.5–3 s per million rows re-sent. On a large outbox, or after the ERROR «failed after it locked
  the outbox», first raise `outbox.recovery.timeout` on the calling instance and restart it. Never delete the
  `<table>_halt` row.
- **Stranded shards** («Застрявшие шарды»): one `POST /recovery/stranded-shards` on the running instances; stop
  nothing, the other shards keep flowing. 409: nothing is stranded, or something holds a lock (call again, what moved
  stays moved), or an xid regression halt is recorded (recover that first). Consumers get those shards' rows again,
  delivered ones too: all of them when the starting tick is gone, those it does not see when only the batch's target is.
- **Waiting records after a final rollback** («Версии контрактов и откат»): `waitingSize` in `/status`, the topic and
  cause in the ERROR line or `outbox.messages.waiting{kind}`. If the release that reads them will not return,
  `waiting/abandon` with `dryRun: true`, then without; body `{"topic", "key"?, "reason", "dryRun"?}`, `key` as UTF-8.
  The audit line carries topic, key, reason and dryRun. Later: redrive with `{"filter":{"topic":"orders",
  "error":"abandoned"}}` once a reading release runs, or discard. A reshard needs them gone first.
- **A key that never gets through** («Ключ, который не проходит никогда»): a record that requeues for ever (`parkedSize`
  stays up) or fails for ever with `maxAttempts = 0` (`outbox.retry.oldest` grows, shard lag does not; the WARN «streams diverted
  to retry» with its error fires once, at the divert). No rollout: `keys/abandon`
  with `dryRun: true`, then without; body `{"topic", "key" | "messageId", "reason", "dryRun"?}`, `key` as UTF-8, `messageId`
  for an unkeyed record (`select message_id from <table>_retry where key is null`). Fix the handler, then redrive the
  letters with `{"filter":{"topic":"orders","error":"abandoned"}}`, or discard them.
- **After a rollback window** («Rolling-деплои», «Обновление с предыдущего релиза (5fc60dd), смешанный парк и
  откат»): the previous release writes dead letters `kept` and the hook never sees them:
  `curl -s -H "$KEY" "$API/dead-letters?status=kept&failedFrom=<start>&failedTo=<end>"`.
