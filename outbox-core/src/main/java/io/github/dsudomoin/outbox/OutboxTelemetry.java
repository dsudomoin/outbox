package io.github.dsudomoin.outbox;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.DoubleSupplier;

/**
 * Metrics and traces of the outbox, implemented by the application's adapter: every method is a no-op by default,
 * and new events come as new default methods. Hooks run on the outbox's own threads, the enqueue ones on the
 * writer's; "the relay thread" below is the thread that steps: a relay worker, or the caller of
 * {@link OutboxRelay#drain()}. An exception a hook throws is logged at WARN (at most once per 10 s per hook) and
 * ignored, so a broken implementation never fails a send or a relay step.
 */
public interface OutboxTelemetry {

    OutboxTelemetry NONE = new OutboxTelemetry() {};

    /** One per {@code send} / {@code sendAll} call, after its transaction check. */
    default Enqueue enqueueStarted() {
        return Enqueue.NONE;
    }

    /**
     * Per message, once its write is committed: after the commit of the business transaction, on the thread that ran
     * it, never for a write rolled back. A {@code send(Connection, ...)} on a connection that is not the current
     * transaction's cannot see its commit: it counts right after the insert.
     */
    default void enqueued(String topic) {}

    /** One per handler call, on the relay thread, before the records are handed to the handler. */
    default Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
        return Delivery.NONE;
    }

    /**
     * One per handler call of the relay {@code step}, the {@link RelayStep} this telemetry's {@link #stepStarted}
     * returned for it (itself, never a wrapper the outbox put around it; {@link RelayStep#NONE} when that returned
     * none or failed), on the relay thread, before the records are handed to the handler. The outbox calls only this
     * one; by default it is {@link #deliveryStarted(List, Instant)}.
     */
    default Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, Instant now) {
        return deliveryStarted(records, now);
    }

    /** A record delivered, {@code delay} after its {@code createdAt}. */
    default void delivered(String topic, Duration delay) {}

    /** A record failed and is retried later. */
    default void retried(String topic) {}

    /** A record moved to the dead table. */
    default void deadLettered(String topic) {}

    /**
     * {@code record} moved to the dead table, on the relay thread, before its step commits. {@code errorType}, a small
     * set that fits a metric label: {@code max_attempts} when its attempts ran out, {@code requeue_limit} when a
     * requeue went past {@code maxRequeues}, else the class name of the error it failed with, as stored. The outbox
     * calls only this one; by default it is {@link #deadLettered(String)} of the record's topic.
     */
    default void deadLettered(OutboxRecord record, String errorType) {
        deadLettered(record.topic());
    }

    /** A record set aside with {@link Outcome#requeue()} and parked. */
    default void requeued(String topic) {}

    /**
     * A record waits for an instance that can handle it ({@link Outcome.Waiting}): neither a delivery nor a failure,
     * it spends no attempt. {@code kind}, a closed set, fits a metric label; the reason stays in the record's error.
     */
    default void waited(String topic, Outcome.Waiting.Kind kind) {}

    /**
     * A send refused with {@link OutboxIncompatibleContractException}, on the writer's thread: some live relay
     * instance cannot read {@code version} of {@code topic}. {@code type}: the class name it was written as, {@code ?}
     * when unknown.
     */
    default void contractRejected(String topic, String type, int version) {}

    /**
     * A send refused with {@link OutboxMissingRouteException}, on the writer's thread: a route of this instance claims
     * {@code topic}, but some live relay instance does not route it.
     */
    default void routeRejected(String topic) {}

    /**
     * The writer guard let a message to {@code topic} through unchecked, on the writer's thread, once per message: this
     * instance has not read the live relay instances' reports lately, and the last read, which the route or the version
     * check took, allowed it. Counted at the guard, before the insert, so the message may still not be written: a
     * later message of the same {@code sendAll} refused, or the insert failing.
     */
    default void routeUnchecked(String topic) {}

    /**
     * A relay step found work, on the thread that steps: called at the first locked shard with work, so only for the
     * steps {@link #stepFinished} measures; a step that locks nothing or finds nothing to do calls neither.
     * {@code shards} is a copy of the shards whose locks the step holds, those without work included. The step's
     * cursor read, its target choice and the retry and parked lookups up to that shard come before this call; its
     * further database work, handler calls ({@link #deliveryStarted(RelayStep, List, Instant)}), its
     * {@link RelayStep#settleStarted settle} and its transaction's commit or rollback run on this thread (the handler
     * itself on a handler thread) until {@link RelayStep#finished()} or {@link RelayStep#failed}.
     */
    default RelayStep stepStarted(List<Integer> shards) {
        return RelayStep.NONE;
    }

    /**
     * A relay step of one or more shards did its work, its writes included, and its transaction commits next:
     * {@code duration} leaves the commit out.
     */
    default void stepFinished(Duration duration) {}

    /**
     * A relay pass, or a step of a single shard, failed and is retried later; the relay logs it at WARN. Never while
     * stopping.
     */
    default void relayFailed(Throwable error) {}

    /** A shard was paused after a failed call. */
    default void shardPaused() {}

    /**
     * The dead-letter hook decided a letter: {@code keep}, {@code discard}, {@code redrive}, {@code redrive_at} or
     * {@code retry_later}.
     */
    default void deadLetterDecision(String decision) {}

    /** A dead-letter hook call failed or timed out. */
    default void deadLetterHookFailed() {}

    /** The inbox saw a message older than its dedup window. */
    default void inboxOutsideWindow() {}

    /**
     * On a handler or dead-letter hook thread after each call there, which then serves other calls: drop state the
     * call may have bound to the thread.
     */
    default void releaseThread() {}

    /**
     * Registers a gauge; {@code value} is read on the caller's schedule, NaN when unknown. A runtime built later around
     * the same telemetry calls this again for the same {@code gauge}: its {@code value} replaces the earlier one.
     */
    default void gauge(OutboxGauge gauge, DoubleSupplier value) {}

    /**
     * Each shard's queue, after every gauge refresh of maintenance, on the instance whose run took the maintenance
     * lock and on its thread: one entry per shard, in shard order. The same read sets {@link OutboxGauge#PENDING} (the
     * sum of the known {@code pending}) and {@link OutboxGauge#PENDING_OLDEST} (the largest known
     * {@code oldestSeconds}), NaN when no shard's is known. Never with {@link #NONE}: that runs no count at all.
     */
    default void queueState(List<ShardQueue> shards) {}

    /**
     * One shard's queue. {@code pending}: its rows written and not yet delivered: those of the main stream beyond its
     * position (a batch in progress: past its {@code last_id}), plus its rows in the retry table, waiting ones
     * included, and in the parked table. {@code oldestSeconds}: the database's {@code now()} less the oldest such
     * row's {@code created_at}, 0 when none. Either NaN when unknown: the shard has no cursor, or a tick it stands on
     * is gone. {@code estimated}: false when {@code pending} is an exact count. The count runs within a budget of
     * 250 ms; past it the main stream is estimated from the ticks, for a shard behind as the ids handed out between
     * its effective tick and the latest divided by the shard count (NaN when a tick has no {@code max_id}), its oldest
     * as the age of its effective tick, for one caught up as none, and the retry and parked rows are counted in a
     * second statement of the same budget, or left out when that runs over too. True also for an unknown entry.
     */
    record ShardQueue(int shard, double pending, double oldestSeconds, boolean estimated) {}

    /** One {@code send} / {@code sendAll} call, in the business transaction. */
    interface Enqueue {

        Enqueue NONE = new Enqueue() {};

        /** The headers to store for a message with {@code headers}; a message with trace context keeps it. */
        default List<OutboxHeader> headers(List<OutboxHeader> headers) {
            return headers;
        }

        /**
         * Once per message of the call, in list order, after the call's checks and before its insert: the message
         * written as the record {@code recordId} ({@link OutboxRecord#messageId()}) on {@code shard}
         * ({@link OutboxRecord#shard()}), both as stored.
         */
        default Message message(OutboxMessage message, UUID recordId, int shard) {
            return Message.NONE;
        }

        /** Every message of the call is inserted. */
        default void finished() {}

        /** The call failed with {@code error}, which reaches the caller. */
        default void failed(Throwable error) {}
    }

    /**
     * One message of an {@link Enqueue}, ended once: {@link #inserted()} or {@link #failed}, before the call's
     * {@link Enqueue#finished()} or {@link Enqueue#failed}. Its commit is {@link #enqueued}.
     */
    interface Message {

        Message NONE = new Message() {};

        /** The headers to store for the message, given those {@link Enqueue#headers} returned. */
        default List<OutboxHeader> headers(List<OutboxHeader> headers) {
            return headers;
        }

        /**
         * Runs {@code insert}, the message's own INSERT, exactly once in the scope of the message; a
         * {@code sendAll} writes its messages in one statement and calls no scope.
         */
        default <T> T inScope(Callable<T> insert) throws Exception {
            return insert.call();
        }

        /** The message is inserted; its transaction has not committed yet. */
        default void inserted() {}

        /** The insert failed with {@code error}, which reaches the caller. */
        default void failed(Throwable error) {}
    }

    /**
     * One relay step, ended once on the thread that started it, with its transaction: {@link #finished()} once its
     * COMMIT returned, {@link #failed} once a failed COMMIT rolled back, or as its own work fails, before the rollback.
     * Its hooks come in this order: {@link #fetched} per shard, then the settle ({@link #settleStarted}, ended before
     * the step), then the end.
     */
    interface RelayStep {

        RelayStep NONE = new RelayStep() {};

        /**
         * Once per shard of the step with work, after the step's reads of that shard and before its main handler call
         * (the calls of its due retry and parked rows come earlier): {@code records}, the records of the shard the step
         * read and handed to the handler, or wrote without a call, each once: its due retry heads with their
         * followers (those a later round of the retry pass promotes included), its due parked rows and the parked rows
         * offered with them, a retry head moved to the dead table out of attempts, its main read (the rows of keys in
         * the retry table and those held out of the call included) and the parked rows merged into that read's call.
         * Rows read and left for a later step without being handed (a head the call has no room for) do not count.
         */
        default void fetched(int shard, int records) {}

        /**
         * Once per step with work, after its last handler call, before it writes the main pass's outcomes and the
         * cursors (the retry pass writes each call's outcomes before the next call, earlier). Ended once, after the
         * step's transaction ends and before this step does.
         */
        default Settle settleStarted() {
            return Settle.NONE;
        }

        /** The step's transaction committed. */
        default void finished() {}

        /** The step failed with {@code error}, which rolls its transaction back (a COMMIT that failed included). */
        default void failed(Throwable error) {}
    }

    /**
     * The writes of one relay step, from its last handler call to its transaction's end. {@code records}: the records
     * whose state the commit records, each once however often the step wrote it (a parked row the retry pass parks
     * again and the main pass merges and writes once more counts once): those handed whose outcome it writes
     * (delivered, retried or diverted to the retry table, dead, parked or requeued, kept parked, postponed), the main
     * read's rows it moves without a call (behind their key in the retry table, or held in the parked table), the
     * parked rows it keeps behind one that stays, and the retry heads it moves to the dead table out of attempts. Not
     * those the next step reads again (the trailing records a call did not attempt, the records of a paused shard),
     * nor the rows a parked row moved to the retry table takes along unread.
     */
    interface Settle {

        Settle NONE = new Settle() {};

        /** After the step's COMMIT returned, before {@link RelayStep#finished()}. */
        default void committed(int records) {}

        /**
         * The step failed with {@code error}, which rolls its transaction back (after a failed COMMIT: once it has),
         * before {@link RelayStep#failed}: none of the {@code records} it would have recorded is; a step whose writes
         * failed part way counts those it made.
         */
        default void failed(int records, Throwable error) {}
    }

    /** One handler call. */
    interface Delivery {

        Delivery NONE = new Delivery() {};

        /** The records the handler gets: the same size and order, with headers replaced as needed. */
        default List<OutboxRecord> handed(List<OutboxRecord> records) {
            return records;
        }

        /**
         * Runs {@code call} on the handler thread in the scope of {@code record}, one of the handed records; see
         * {@link OutboxHandler#inRecordScope}.
         */
        default <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            return call.call();
        }

        /**
         * On the handler thread, as soon as the handler knows the outcome of {@code record}, one of the handed records
         * (the same object): {@link OutboxHandler#recordFinished}, which the default {@code handleBatch} calls after
         * each record it attempts. At most once per record from a well-behaved handler, before {@link #finished}; a
         * handler may also report twice, or a record not of this call. A call the relay cancelled at
         * {@code batchTimeout} keeps this delivery bound to its handler thread until its {@code handleBatch} returns,
         * so a report may arrive while {@link #finished} or {@link #failed} runs on the relay thread, or after the
         * step's {@link RelayStep#finished()}: an implementation must be thread-safe and let the first end of a record
         * win. The report is the handler's word: {@link #finished} still follows with the outcome of every record, and
         * that outcome, not this one, decides the record's row (a call that fails as a whole retries a record reported
         * delivered).
         */
        default void recordFinished(OutboxRecord record, Outcome outcome) {}

        /** The call returned {@code outcomes}, one per record in order (a timed-out call's are retries). */
        default void finished(List<Outcome> outcomes) {}

        /** The call ended without outcomes: interrupted, or the relay is closing. */
        default void failed(Throwable error) {}
    }
}
