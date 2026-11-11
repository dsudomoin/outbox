package io.github.dsudomoin.outbox;

import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.UUID;

/**
 * The morning tool, for any code of the service (an endpoint, a job, a test): the outbox's status, dead letters, kept
 * orphans, the xid-regression recovery, stranded shards and waiting records. Every call manages its own transactions,
 * so it fails with {@link IllegalStateException} inside the application's transaction; any instance gives the same
 * answers, as all act on the shared database.
 *
 * <p>Bulk calls act on the letters dead when they began, none that die meanwhile (a redriven letter dying again
 * included), in chunks of 1 000 letters in id order, one transaction each, so a large redrive never holds more than
 * 1 000 key locks. A failing chunk rolls back alone: the chunks before it stay done, and calling again goes on.
 * They skip, without waiting and without counting them, the letters a running dead-letter hook call holds and, for a
 * redrive, those whose (topic, key) an open transaction is sending: call again for those. Each mutating call logs its
 * arguments and result in one INFO line, or in one WARN when it fails after it began.
 */
public interface OutboxAdmin {

    /** Read from the database now. */
    OutboxStatus status();

    /**
     * Up to {@code limit} (1..1000) letters matching {@code filter} with an id above {@code afterId} (null: from the
     * first), in id order.
     */
    DeadLetterPage findDeadLetters(DeadLetterFilter filter, @Nullable Long afterId, int limit);

    long countDeadLetters(DeadLetterFilter filter);

    /**
     * Sends these letters back into the outbox, whatever their status, as {@link DeadLetterDecision#redrive()} does: a
     * new message after the later messages of its (topic, key), with the same message id, topic, key, payload and
     * headers, attempts reset, the dead row deleted in the same transaction; should it die again within
     * {@code outbox.deadLetters.redriveHistory}, its {@link DeadLetter#redrives()} is one higher. Wakes the relay.
     * Returns how many were redriven: ids of no letter count for nothing.
     */
    long redriveDeadLetters(Collection<Long> ids);

    /**
     * {@link #redriveDeadLetters(Collection)} for the letters matching {@code filter}.
     *
     * @throws IllegalArgumentException for an empty filter, or a blank topic or errorContains: name what to redrive
     */
    long redriveDeadLetters(DeadLetterFilter filter);

    /** Deletes these letters; returns how many. */
    long discardDeadLetters(Collection<Long> ids);

    /**
     * Deletes the letters matching {@code filter}; returns how many.
     *
     * @throws IllegalArgumentException for an empty filter, or a blank topic or errorContains: name what to discard
     */
    long discardDeadLetters(DeadLetterFilter filter);

    /**
     * Moves the rows of {@code table}, an orphan of {@link OutboxStatus#orphans()}, with an xid at or above
     * {@code fromXid} back into the outbox, then drops it: <b>its rows below {@code fromXid} go with the table</b>.
     * Moved rows are new messages, with new ids, so they are delivered after later messages of their keys, and the
     * same message ids. Runs as no maintenance run or other recovery call does at the same time: waits up to 60 s for
     * one in progress, then fails with {@link IllegalStateException}
     * "maintenance or another outbox admin recovery call is running, retry". A restore that fails midway loses and
     * repeats nothing; calling again goes on. Wakes the relay; returns the rows moved.
     *
     * @throws IllegalArgumentException before any SQL that names it, unless {@code table} is {@code <base>_x<digits>}
     *     or {@code <base>_o<digits>}, unqualified or qualified as the outbox is named, a table in the outbox's schema
     *     in no partition tree, and not one the partition extension made ahead of the tail for its next swap; and for
     *     a negative {@code fromXid} ({@code 0} moves every row)
     */
    long restoreOrphan(String table, long fromXid);

    /**
     * Runs the xid-regression recovery now, the one maintenance runs by itself with
     * {@code outbox.recovery.xidRegression = auto}, whatever that mode and {@code outbox.recovery.maxRows}: for a halt
     * in {@code halt} mode or above {@code maxRows}. One transaction re-sends the rows the relay may not have
     * delivered, as new messages with the same message ids (consumers drop within their inbox window the ones delivered
     * before); writers wait for it, 2.5 to 3 s per million rows in the library's measurement. A failed recovery rolls
     * back. Runs as no maintenance run or other recovery call does at the same time: waits up to 60 s for one in
     * progress, then fails with {@link IllegalStateException}
     * "maintenance or another outbox admin recovery call is running, retry". Once the recovery committed the call
     * returns the rows re-sent, and a failure after it is a WARN. A failed commit may still have gone through: when a
     * read on a new connection then finds the recovery done, the call returns -1, the count unknown. Wakes the relay.
     *
     * @throws IllegalStateException "no xid regression to recover" when the xid counter is at or above every tick's
     *     xmax and no halt is recorded; "outbox.shards = N but ... cursors for M shards" when the shard count changed
     *     since the cursors were made: calling again does not help until {@code outbox.shards} is set back to M; "the
     *     detach of ... became pending" when someone began a partition detach while the call ran: calling again
     *     retries, finishing it first; "... failed at its commit, outcome unknown, call again" when the commit failed
     *     and the recovery is not found done: a repeat recovers, or is refused with "no xid regression to recover"
     *     if the commit went through after all
     */
    long recoverXidRegression();

    /**
     * Repairs the stranded shards, those starting from a tick that no longer exists or with a batch in progress running
     * to one (the ERROR "outbox shards ... start from ticks ... that no longer exist"), on a running fleet: the other
     * shards keep flowing and nothing has to stop. Which of a stranded shard's rows went through the relay is gone with
     * its tick, so each of its rows still in the outbox (when its starting tick is left, each that tick does not see)
     * goes back as a new message, with a new id and xid and the same message id, delivered after later messages
     * of its key: consumers get as duplicates the ones delivered before, and only those inside their inbox window are
     * dropped for them. Rows in the retry, parked and dead tables stay there. Then the shard's cursor moves to the
     * latest tick. A transaction committing meanwhile loses nothing: what the tick does not cover the relay delivers.
     *
     * <p>Under the maintenance lock, for the whole call, so maintenance runs on every instance are skipped meanwhile:
     * waits up to 60 s for a maintenance run or another recovery call in progress, then fails with
     * {@link IllegalStateException} "maintenance or another outbox admin recovery call is running, retry". Shard by
     * shard, in chunks of 1 000 rows, one transaction each under the shard's lock, the one a relay step holds, waited
     * for up to {@code outbox.relay.batchTimeout} + 5 s. A key another transaction holds (an open {@code send} of it)
     * is waited for last. Whatever keeps a lock too long fails the call with {@link IllegalStateException} "... busy,
     * retry": the rows moved stay moved, each once, and calling again goes on. Wakes the relay; returns the rows
     * re-sent.
     *
     * @throws IllegalStateException "no stranded shards" when no shard is stranded; "recover the xid regression first"
     *     while an xid regression halt is recorded: that recovery rebuilds every cursor
     */
    long recoverStrandedShards();

    /**
     * Gives up on the records of {@code topic} that wait for an instance able to handle them ({@link Outcome.Waiting}),
     * of {@code key} only unless it is null: for a rollback that is final. The whole key goes, so no record of it runs
     * without the one before it: each waiting head, the records of its key queued behind it and its key's requeued
     * records become dead letters, kept (the dead-letter hook is not asked), with the error {@code "abandoned: "} +
     * {@code reason}; an unkeyed head goes alone. Records of the key sent later are delivered, ahead of the abandoned
     * ones. Returns the keys (an unkeyed head counts as one) and records moved; with {@code dryRun}, those it would
     * move, moving nothing.
     *
     * <p>Shard by shard, in chunks of 1 000 keys, one transaction each under the shard's lock, the one a relay step
     * holds while it runs: it waits up to {@code outbox.relay.batchTimeout} + 5 s for it, then fails with
     * {@link IllegalStateException} "... busy, retry", the chunks before it staying done; calling again goes on. A dry
     * run takes no lock.
     *
     * @throws IllegalArgumentException for a blank {@code topic} or {@code reason}
     */
    Abandoned abandonWaiting(String topic, @Nullable byte[] key, String reason, boolean dryRun);

    /**
     * Gives up on the records of {@code key} that a handler will never take: one that requeues for ever, or fails for
     * ever with {@code maxAttempts = 0}. Every row of the key in the retry and parked tables, whether or not its head
     * waits, becomes a dead letter, kept (the dead-letter hook is not asked), with the error {@code "abandoned: "} +
     * {@code reason}. Records of the key not read yet, and those sent later, are delivered, ahead of the abandoned ones.
     * Returns one key and its records moved, or nothing when it has none; with {@code dryRun}, those it would move,
     * moving nothing.
     *
     * <p>In chunks of 1 000 rows, the newest first, one transaction each under the shard's lock, the one a relay step
     * holds while it runs: what a failed call leaves still has its head. It waits up to
     * {@code outbox.relay.batchTimeout} + 5 s for it, then fails with {@link IllegalStateException} "... busy, retry",
     * the chunks before it staying done; calling again goes on. A dry run takes no lock.
     *
     * @throws IllegalArgumentException for a blank {@code topic} or {@code reason}, or a null {@code key}
     */
    Abandoned abandonKey(String topic, byte[] key, String reason, boolean dryRun);

    /**
     * {@link #abandonKey} for an unkeyed record, which has no key to name: its row in the retry or parked table.
     *
     * @throws IllegalArgumentException as {@link #abandonKey}, or when the record has a key: it cannot go without the
     *     rest of its key, so name that
     */
    Abandoned abandonRecord(String topic, UUID messageId, String reason, boolean dryRun);

    /** What an abandon moved, or would move: the keys, an unkeyed record counting as one, and all their records. */
    record Abandoned(long keys, long records) {}
}
