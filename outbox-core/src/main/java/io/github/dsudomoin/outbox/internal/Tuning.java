package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxConfig;
import java.time.Duration;

/**
 * The library's internal timings and sizes: fixed in production, not configuration. A config object that also
 * implements this interface overrides them; only this repository's tests and prodcheck do. Not validated, except where
 * a check combines one with a user key ({@link OutboxSchema#validate}, {@link InboxPartitions}).
 */
public interface Tuning {

    Tuning DEFAULTS = new Tuning() {};

    /**
     * {@code config}'s tuning: its own when it implements Tuning (also through {@link OutboxSchema.Resolved}), else
     * {@link #DEFAULTS}.
     */
    static Tuning of(OutboxConfig config) {
        if (config instanceof OutboxSchema.Resolved r) {
            return of(r.config());
        }
        return config instanceof Tuning t ? t : DEFAULTS;
    }

    /**
     * The shard count of a new outbox, at most {@link Short#MAX_VALUE}; an existing one keeps the count of its cursors
     * ({@link OutboxSchema#shards}).
     */
    default int shards() {
        return 256;
    }

    /**
     * How long the library's own connections wait on a silent socket ({@code Connection.setNetworkTimeout}) when they
     * have no such bound of their own, as pgjdbc's default {@code socketTimeout = 0}; 0: no bound. The heartbeat takes
     * at most its interval, at least 10 s; a statement under a {@code statement_timeout} of the library's gets that long
     * instead.
     */
    default Duration networkTimeout() {
        return Duration.ofMinutes(5);
    }

    /** The most ticks one batch may span. */
    default int maxTicksPerBatch() {
        return 100;
    }

    /** Under load, ticks come at most once per this; a commit waits for its tick up to about twice it. */
    default Duration minTickInterval() {
        return Duration.ofMillis(50);
    }

    /**
     * How often idle ticks come (about one per interval for the whole cluster), and how soon a commit on an instance
     * without a local wake-up is noticed.
     */
    default Duration idlePollInterval() {
        return Duration.ofMillis(200);
    }

    /**
     * How long a relay worker starts steps on one pooled connection under a backlog before it hands it back, so the
     * pool can retire it at its maxLifetime. A step started just before this ends runs on the same connection, up to 4
     * × {@code batchTimeout}: a pool's leak detection threshold must exceed connectionHold + 4 × batchTimeout.
     */
    default Duration connectionHold() {
        return Duration.ofMinutes(1);
    }

    /**
     * With the relay disabled, how often this instance makes a tick while it writes, so a backlog written while no relay
     * runs comes in batches instead of one tick; 0: no ticks. Ignored while the relay runs.
     */
    default Duration ticksWithoutRelay() {
        return Duration.ofSeconds(1);
    }

    /** Failed attempts in a row after which a shard reads one head row and diverts the single failing key. */
    default int headFailuresBeforeDivert() {
        return 3;
    }

    /**
     * How long an idle shard's latest cursor row may stay on a tick below its effective one before maintenance appends
     * one at the effective tick (an anchor). At least {@link #maintenanceInterval()}.
     */
    default Duration cursorAnchorInterval() {
        return Duration.ofHours(1);
    }

    /** A requeued record is due this later, or at the handler's later {@code notBefore}. */
    default Duration requeueDelay() {
        return Duration.ofSeconds(1);
    }

    /**
     * A record waiting for an instance that can handle it is handed again after a quarter of the time it has waited, at
     * least this and at most {@link #waitMaxDelay()} later.
     */
    default Duration waitDelay() {
        return Duration.ofSeconds(1);
    }

    default Duration waitMaxDelay() {
        return Duration.ofMinutes(5);
    }

    /** A record waiting longer turns the periodic WARN about waiting records into an ERROR. */
    default Duration waitAlertAfter() {
        return Duration.ofMinutes(15);
    }

    /** The size of one xid range partition (2^24 xids). */
    default long xidsPerPartition() {
        return 16_777_216L;
    }

    /** Ranges kept above the current xid; maintenance extends them when half is used. */
    default int premake() {
        return 64;
    }

    /** The period of the outbox's and the inbox's maintenance and of each instance's heartbeat. */
    default Duration maintenanceInterval() {
        return Duration.ofSeconds(30);
    }

    /**
     * How long the tail swap waits in the queue for its lock on the outbox once its tries without waiting failed,
     * writers queueing behind it meanwhile. 0: no wait, so writers wait only for the swap's hold.
     */
    default Duration swapLockTimeout() {
        return Duration.ZERO;
    }

    /**
     * Largest tail, in bytes ({@code pg_relation_size}), that the swap turns into a range when rows went to it, which
     * scans it under the swap's lock: writers wait for the scan. A larger tail stays hot until
     * {@code outbox.partitions.tailConversion.window}.
     */
    default long maxTailScan() {
        return 16L * 1024 * 1024;
    }

    /** The time span of one inbox range (by UUIDv7 time), a whole number of minutes. */
    default Duration inboxPartitionInterval() {
        return Duration.ofHours(1);
    }

    /** Inbox ranges are kept up to now + this. */
    default Duration inboxPremake() {
        return Duration.ofHours(24);
    }

    /** Letters per call of the dead-letter hook, at most {@link PgOutboxAdmin#CHUNK} (a redrive holds a key lock each). */
    default int deadLetterBatchSize() {
        return 100;
    }

    /** Bound of one dead-letter hook call; a call cut at it leaves its letters pending, retried with backoff. */
    default Duration deadLetterTimeout() {
        return Duration.ofSeconds(130);
    }

    /** How long the count of a message's redrives is remembered, so a redriven message that dies again carries it. */
    default Duration redriveHistory() {
        return Duration.ofDays(30);
    }

    /**
     * {@code auto}: the instance holding the maintenance lock recovers an xid regression by itself, when at most
     * {@link #recoveryMaxRows()} rows are to be re-sent; {@code halt}: never by itself, the relay stays halted until
     * {@code OutboxAdmin.recoverXidRegression()}.
     */
    default String xidRegression() {
        return "auto";
    }

    /** Rows to re-send above which the automatic recovery leaves the relay halted. */
    default long recoveryMaxRows() {
        return 1_000_000L;
    }

    /** Rows written since the newest pre-regression tick, less this margin for clock skew, are re-sent. */
    default Duration recoveryClockMargin() {
        return Duration.ofMinutes(10);
    }
}
