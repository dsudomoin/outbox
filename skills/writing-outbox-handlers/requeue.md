# Requeue: set a record aside without blocking its key

`Outcome.requeue()` means «not now: let the rest of my key go, hand me back later». Use it when the record is not
ready but the target is fine, typically a handler that runs a per-entity job queue (key = account id) and finds
another process of the same account running, a process that ends on its own.

| | `Outcome.retry(e)` | `Outcome.requeue()` |
|---|---|---|
| Means | the target failed | the record is not ready; the target is fine |
| Its key | waits: later records queue behind it in `<table>_retry` | goes on: later records are delivered while it waits |
| Comes back | after backoff (1 s doubling to 5 min), or at `notBefore` if later | whichever comes first: its due time `now + outbox.relay.requeue.delay`, or, while the key's parked records fit in `batchSize`, right before the next new record of its key (in the same call) (1 s); with `requeue(notBefore)`, `max(now + delay, notBefore)`, at most 24 h ahead. A record whose time comes brings the older parked records of its key along, not yet due as they may be, so `notBefore` is no lower bound |
| Counts | `record.attempts()`, `relay.retry.maxAttempts` | `record.requeues()`, `relay.requeue.maxRequeues` |
| Shard | pauses and backs off when nothing was delivered | no pause, no backoff, chunk size unchanged |

Never re-send a copy with `Outbox.send` from the handler instead: `handleBatch` runs outside the relay step's
transaction, so a crash between the two leaves an extra copy. `requeue` moves the record into `<table>_parked` in the
step's own transaction (same `message_id`, payload, headers, `created_at`); a rolled-back step moves nothing.

## The coordinator

Only a `handleBatch` override can requeue: the default `handleBatch` (calling `handle` per record) never returns it.
Topic routing (`OutboxTopicHandler`, `OutboxHandler.byTopic`) passes a `requeue` through like any outcome.

```java
/** Per-account job queue: topic "account-jobs", key = account id. A @Component in Kora. */
public final class AccountJobsHandler implements OutboxTopicHandler {

    private static final int GIVE_UP_AFTER = 1000; // requeues, then dead letters

    private final AccountProcesses processes; // which processes of which accounts run now
    private final AccountJobs jobs;

    public AccountJobsHandler(AccountProcesses processes, AccountJobs jobs) {
        this.processes = processes;
        this.jobs = jobs;
    }

    @Override
    public Set<String> topics() {
        return Set.of("account-jobs");
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        jobs.start(record);
    }

    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        var outcomes = new ArrayList<Outcome>(records.size());
        var failedKeys = new HashSet<ByteBuffer>();
        var heldTypes = new HashMap<ByteBuffer, Set<String>>(); // per key, the job types held back in this call
        for (var record : records) {
            // an unkeyed record: its failure holds back nothing
            var key = record.key() == null ? null : ByteBuffer.wrap(record.key());
            if (key != null && failedKeys.contains(key) || OutboxHandler.windDownRequested()) {
                outcomes.add(Outcome.notAttempted());
            } else if (key != null && heldTypes.getOrDefault(key, Set.of()).contains(jobs.type(record))
                    || processes.blocks(record)) { // e.g. a running WITHDRAW of the account blocks its FEE_RELEASE
                outcomes.add(record.requeues() < GIVE_UP_AFTER
                        ? Outcome.requeue()
                        : Outcome.permanent(new IllegalStateException("blocked " + record.requeues() + " times")));
                if (key != null) { // a later FEE_RELEASE of this account in the call waits too
                    heldTypes.computeIfAbsent(key, k -> new HashSet<>()).add(jobs.type(record));
                }
            } else {
                try {
                    handle(record);
                    outcomes.add(Outcome.delivered());
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    outcomes.add(Outcome.retry(e));
                    if (key != null) {
                        failedKeys.add(key);
                    }
                }
            }
        }
        return outcomes;
    }
}
```

- The `handleBatch` contract still holds: once a key fails in the call, its later records are `notAttempted()`. A
  `requeue` there is not applied: the record follows the failed one into `<table>_retry`.
- Once a record of a key is requeued in a call, requeue that key's later records of the same type in the call too, or
  FEE_RELEASE#2 overtakes a requeued FEE_RELEASE#1 when the blocking process ends mid-call; other types still go
  through (a requeued SYNCHRONIZE#2 does not hold back FEE_RELEASE).
  This is safe only where no later record of that type carries what the older one waits for. Otherwise the handler
  holds the very record that would end the wait, and the key stands for ever (with `maxRequeues = 0` nothing stops it):
  make the type tell «waits for a result» from «carries a result».
- Use `requeue(notBefore)` when the handler knows when the blocking process ends. `notBefore` is by the JVM's clock
  (`Instant.now().plusSeconds(3)` means 3 s on the database's clock, however far off this instance's is), and it is no
  lower bound: a newer record of the key, or its due time, can bring the record back sooner.
- Give up either in the handler (`Outcome.permanent(e)` past a `record.requeues()` bound, per route) or globally
  with `outbox.relay.requeue.maxRequeues` (default 0 = unlimited): the requeue past it moves the record to
  `<table>_dead` with the error «requeue limit reached», attempts unchanged. Every hand-back answered with `requeue`
  counts, a merge ahead of a new record of the key and an early offer ahead of a newer due record of the key included,
  so a busy key spends requeues faster than one per
  `delay`, at a rate you cannot predict. To give up after a time instead, use `record.createdAt()`:
  `Duration.between(record.createdAt(), Instant.now())` past a limit → `Outcome.permanent(e)`.

## Order

Per key, first delivery follows commit order, except that a requeued record is delivered after the records of its key
that were delivered while it was parked. One rule on every path, whether a new record, a retry head or a due time starts
the call: a key whose parked records fit in `batchSize` comes whole and in order, and a larger key brings no older records,
so its record may pass older ones, each of which was offered and answered `requeue`. FEE_RELEASE#1 requeued, then
FEE_RELEASE#2 arrives → one call [#1, #2]; not-due records come with the rest, so a `notBefore` is no promise. A larger key
comes in portions in the order they came due, due `D = delay × ceil(N / batchSize)` later (the turn takes the longer of `D` and
`N / batchSize ×` the step time, main call included), as a later record may
carry what the older ones wait for. When a key's offered records have all left, its next `batchSize` come at once, in order,
whatever their due times, a `notBefore` included; where the key's oldest just answered `requeue` and newer ones left, they are
offered again at most once per `delay`. An unkeyed record comes back only at its due time, alone.

## When a parked record is handed back

| Outcome | Effect |
|---|---|
| `delivered()` | Leaves `<table>_parked`. |
| `requeue()` | Stays parked, `requeues() + 1`, new due time. |
| `notAttempted()` | Stays parked, due no earlier than `now + delay`; no attempt spent. |
| `retry(e)` | Becomes the retry head of its key: the key is blocked from then on, and its newer parked records follow it. `maxAttempts` applies as to any record. |
| `permanent(e)` | `<table>_dead`. |

A `requeue` of a retry-table head parks it and lets the next record of its key go on.

## Call size and wind-down

- A call carries up to 2 × `batchSize` records: a chunk or due rows, with the parked records of their keys. Where a whole
  key does not fit, its new records go straight to `<table>_parked` behind its older ones, due at once, with no call and no
  requeue counted; the chunk, its cursor and other keys go on, and the key comes whole next. A retry head waits for the next
  step. Keep the blocked check
  cheap, or size the batch budget in SKILL.md for 2 ×.
- The order rule gives way in three places. Two calls isolate a culprit and so carry one group: a probing shard's call is
  the head row alone, which may go out ahead of its key's parked records, and after a retry call that failed as a whole each
  group goes alone, the head with its own rows first. Third, a parked row of a shard whose `parked_due_at` is empty (written
  by an instance of the previous release or by hand) is not merged until maintenance repairs the due time.
- The chunk limit halves after a failure, the parked records of a key still come whole: a call that times out because of them
  can cost up to 3 timeouts, with the shard paused, until the probe sends the head alone.
- A call in which the handler only requeued (or left not attempted) the merged parked records and attempted none of
  the new ones fails as a whole, like a call with nothing attempted: shard pause, halved chunk, probe, divert. A
  handler that winds down right after requeuing costs a pause, never a spinning shard.
- A call that pauses the shard applies none of its requeues: those records are handed again with the chunk.

## Observing it

Counter `outbox.messages.requeued{topic}` (every requeue), gauge `outbox.parked.size`, span outcome `requeued`
(operating-outbox, [metrics.md](../operating-outbox/metrics.md)). In tests `OutboxRelay.drain()` does not
wait for a parked record that is not due yet: assert `select count(*) from <table>_parked` = 0 next to `_retry`.
