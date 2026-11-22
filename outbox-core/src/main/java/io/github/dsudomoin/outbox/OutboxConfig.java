package io.github.dsudomoin.outbox;

import java.time.Duration;

/**
 * The outbox's settings; every one has a default: implement it and override what you change
 * ({@code new OutboxConfig() {}} is all defaults). The Kora adapter reads it from HOCON path {@code outbox}, keys named
 * as here.
 */
public interface OutboxConfig {

    default String table() {
        return "outbox";
    }

    default RelayConfig relay() {
        return new RelayConfig() {};
    }

    default PartitionsConfig partitions() {
        return new PartitionsConfig() {};
    }

    default InboxConfig inbox() {
        return new InboxConfig() {};
    }

    default DeadLettersConfig deadLetters() {
        return new DeadLettersConfig() {};
    }

    default RecoveryConfig recovery() {
        return new RecoveryConfig() {};
    }

    default SchemaConfig schema() {
        return new SchemaConfig() {};
    }

    interface RelayConfig {

        default boolean enabled() {
            return true;
        }

        default int workers() {
            return 4;
        }

        /**
         * Rows per chunk read and per handler call. Keep {@code batchSize ×} the handler's time per record well below
         * {@link #batchTimeout()}: a longer call is cut at its soft deadline and the shard moves on only by the records
         * the handler finished. A handler that spends milliseconds on each record (one request per record) sets it to
         * 1000 or lower: at 10 000 and 10 ms a record its call takes 100 s, about the soft deadline of the default
         * {@link #batchTimeout()}, and a failed step hands all of its records again. At 10 000, a shard whose recent
         * rows exceed about 420 bytes is read by the byte-budgeted statement ({@link #batchBytes()}). Measured against
         * 1000 on a Postgres 1 ms away, 4 workers, 256 shards: 300-byte rows drained about 2.4 times faster, 1 KB rows
         * in the database's memory about 1.2 times, a backlog larger than its memory within 10 % of 1000.
         */
        default int batchSize() {
            return 10_000;
        }

        /**
         * Bytes per chunk read and per handler call, as {@link #batchSize()} bounds its rows: a row counts its payload,
         * key and headers as stored (a null counts nothing), and a call takes rows while they fit, at least one, so a
         * row larger than this goes alone. Parked and retry rows of a call get as much again, so a call holds at most
         * about twice this; each worker holds about one call, and the driver keeps another copy while it reads.
         * While a shard's recent rows are small ({@code batchSize} of the largest below a quarter of this: about 420
         * bytes at the defaults), its chunk goes by a plain read without the byte count, so a sudden jump in message
         * size can make one read per worker past this; the first read of each shard after a start or a worker's fatal
         * error is always bounded. 0: no byte limit.
         */
        default long batchBytes() {
            return 16L * 1024 * 1024;
        }

        /**
         * Bound of one handler call. At 80 % of it the handler thread is interrupted and the call's real outcomes are
         * kept (records it did not start are read again); a call still running at the full timeout is cancelled and
         * all its records are retried, delivered ones included. Per-record timeouts of the handler (HTTP read
         * timeouts, each {@code byTopic} route's own) must be well below it. Default 130 s, the Kafka sender's
         * budget with its default producer settings ({@code delivery.timeout.ms} 120 s + 5 s + {@code max.block.ms}
         * 5 s); a handler without such long calls may set it lower. At most 24 days. See
         * {@link OutboxHandler#handleBatch}.
         */
        default Duration batchTimeout() {
            return Duration.ofSeconds(130);
        }

        /**
         * On stop, unfinished batches get this long, then their workers are interrupted and the batches are delivered
         * again later; a step still waiting for its handler then (a Kafka call through an outage, which may take
         * {@link #batchTimeout()}) is interrupted too, and its records go again. Default 30 s.
         */
        default Duration shutdownTimeout() {
            return Duration.ofSeconds(30);
        }

        default RetryConfig retry() {
            return new RetryConfig() {};
        }

        default RequeueConfig requeue() {
            return new RequeueConfig() {};
        }
    }

    interface RetryConfig {

        default Duration initialDelay() {
            return Duration.ofSeconds(1);
        }

        default Duration maxDelay() {
            return Duration.ofMinutes(5);
        }

        /** 0 = retry forever. */
        default int maxAttempts() {
            return 0;
        }

        /**
         * Whether a {@link Outcome.Permanent} the handler returns stops its (topic, key) instead of moving the record
         * to the dead table. Off (the default): the record is dead and its key goes on with the next record. On: the
         * relay takes every {@code Permanent} as a {@link Outcome.Retry} with the same error, so the record becomes
         * the retry head of its key and the later records of the key wait behind it; it spends attempts, backs off up
         * to {@link #maxDelay()} and shows in the {@code outbox.retry.oldest} gauge, and each handler call that held a
         * record logs an ERROR. With {@link #maxAttempts()} above 0 such a record still goes dead after that many
         * attempts, as any retried one does: a key is never skipped past its record only with {@code maxAttempts = 0}.
         * The key moves on once the cause is fixed and the next retry delivers the record, or once
         * {@link OutboxAdmin#abandonKey} gives it up. The default {@link OutboxHandler#handleBatch} and the Kafka
         * sender then skip the rest of the key in the call, as after a Retry; a {@code handleBatch} of your own must
         * answer {@code NotAttempted} for the later records of a key it answered {@code Permanent} for, or they reach
         * the target ahead of it and are handed again behind it. The Kafka sender sends such calls with one request in
         * flight per broker connection, whatever {@code max.in.flight.requests.per.connection} is set to, so a large
         * call is slower.
         */
        default boolean holdPermanent() {
            return false;
        }
    }

    /** Records the handler set aside with {@link Outcome#requeue()}. */
    interface RequeueConfig {

        /** Requeues a record may take; the one past it moves the record to the dead table. 0 = unlimited. */
        default int maxRequeues() {
            return 0;
        }
    }

    interface PartitionsConfig {

        default TailConversionConfig tailConversion() {
            return new TailConversionConfig() {};
        }
    }

    /** When a hot tail over the swap's scan limit (16 MB) is turned into a range all the same: writers wait for it. */
    interface TailConversionConfig {

        /**
         * {@code HH:mm-HH:mm} in the JVM's default time zone, crossing midnight when the end comes first; empty: never.
         * Default {@code 03:00-05:00}.
         */
        default String window() {
            return "03:00-05:00";
        }

        /**
         * The scan's {@code statement_timeout} in the window, so about the longest writers wait for its hold: keep it
         * below their own {@code lock_timeout} and {@code statement_timeout}. A scan that does not fit rolls back,
         * nothing left behind, and this instance tries again in the next window. Default 1 s, at most 24 days.
         */
        default Duration maxStall() {
            return Duration.ofSeconds(1);
        }

        /** The IANA time zone {@link #window()} is read in. Default: the JVM's default zone. */
        default String zone() {
            return java.time.ZoneId.systemDefault().getId();
        }
    }

    interface InboxConfig {

        default String table() {
            return "inbox";
        }

        default Duration retention() {
            return Duration.ofHours(24);
        }
    }

    interface DeadLettersConfig {

        /**
         * Kept letters older than this, by their {@code failed_at}, are deleted by maintenance; 0 = kept forever, the
         * default; at most 3650 days. Letters still pending for the hook are never deleted. Deleted in portions, a
         * maintenance run spending at most its 30 s interval on them with the redrive history's cleanup; later runs
         * finish the rest.
         */
        default Duration retention() {
            return Duration.ZERO;
        }
    }

    /** What happens when the xid counter is found below a tick's xmax (a logical restore or migration). */
    interface RecoveryConfig {

        /**
         * The whole recovery transaction's deadline, writers waiting for it all along; each statement gets what is
         * left. Default 10 min, at most 24 days.
         */
        default Duration timeout() {
            return Duration.ofMinutes(10);
        }
    }

    interface SchemaConfig {

        /**
         * True (default): every instance creates and upgrades the base tables at startup. False: the application's
         * migrations own them ({@link OutboxSql}), and startup only checks them and the roles' rights, failing with
         * every problem named; partitions are the library's in both modes.
         */
        default boolean bootstrap() {
            return true;
        }
    }
}
