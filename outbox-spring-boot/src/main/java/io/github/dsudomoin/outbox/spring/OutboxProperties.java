package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.OutboxConfig;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.ZoneId;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The outbox's settings under {@code outbox.*}, keyed as the Kora adapter's HOCON (kebab-case canonical, camelCase
 * binds too), with {@link OutboxConfig}'s defaults. Durations take Spring's formats ({@code 30s}, {@code 1h},
 * {@code PT30S}, a bare number is milliseconds); HOCON's {@code 30 seconds} does not parse.
 *
 * @param table the base name of all the outbox's tables, [schema.]name; without a schema, in current_schema().
 *     Default outbox.
 * @param relay the relay that hands the outbox's records to the handlers.
 * @param partitions the outbox table's partitions.
 * @param inbox the inbox table that deduplicates handled messages.
 * @param deadLetters records moved to the dead table and the dead-letter hook.
 * @param recovery what happens when the xid counter is found below a tick's xmax (a logical restore or migration).
 * @param schema the outbox's base tables.
 * @param enabled false: no outbox runtime, the inbox only. Default true.
 * @param json the JSON mapper of typed payloads.
 * @param heartbeat the connections of the heartbeat, which keeps this instance's row fresh and reads the reports that
 *     guard routed sends.
 */
@ConfigurationProperties("outbox")
public record OutboxProperties(
    @DefaultValue("outbox") String table,
    @DefaultValue Relay relay,
    @DefaultValue Partitions partitions,
    @DefaultValue Inbox inbox,
    @DefaultValue DeadLetters deadLetters,
    @DefaultValue Recovery recovery,
    @DefaultValue Schema schema,
    @DefaultValue("true") boolean enabled,
    @DefaultValue Json json,
    @DefaultValue Heartbeat heartbeat
) implements OutboxConfig {

    /**
     * The relay that hands the outbox's records to the handlers.
     *
     * @param enabled false: this instance runs no relay workers, dead-letter pass and partition maintenance;
     *     OutboxRelay.drain() and the instance's heartbeat still work, and while it writes it makes a tick every
     *     second. Keep at least one instance with the relay on.
     *     Default true.
     * @param workers relay threads per instance (outbox-relay-N), at least 1; each holds a database connection while it
     *     works. Default 4.
     * @param batchSize rows per chunk read and per handler call. Keep batchSize × the handler's time per record well
     *     below batch-timeout: a longer call is cut at its soft deadline and the shard moves on only by the records the
     *     handler finished. A handler that spends milliseconds on each record sets it to 1000 or lower. At 10000, a
     *     shard whose recent rows exceed about 420 bytes is read by the byte-budgeted statement.
     *     Default 10000.
     * @param batchBytes bytes per chunk read and per handler call, as batch-size bounds its rows: a row counts its
     *     payload, key and headers as stored, and a call takes rows while they fit, at least one, so a row larger than
     *     this goes alone. Parked and retry rows of a call get as much again, so a call holds at most about twice this.
     *     0: no byte limit. Default 16 MB.
     * @param batchTimeout bound of one handler call. At 80 % of it the handler thread is interrupted and the call's
     *     real outcomes are kept (records it did not start are read again); a call still running at the full timeout
     *     is cancelled and all its records are retried, delivered ones included. Per-record timeouts of the handler
     *     (HTTP read timeouts, each byTopic route's own) must be well below it. Default 130 s, the Kafka sender's
     *     budget with its default producer settings (delivery.timeout.ms 120 s + 5 s + max.block.ms 5 s); a handler
     *     without such long calls may set it lower. At most 24 days.
     * @param shutdownTimeout on stop, unfinished batches get this long, then their workers are interrupted and the
     *     batches are delivered again later; a step still waiting for its handler then (a Kafka call through an
     *     outage) is interrupted too, and its records go again. Default 30 s.
     * @param retry the backoff of failed records.
     * @param requeue records the handler set aside with Outcome.requeue().
     */
    public record Relay(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("4") int workers,
        @DefaultValue("10000") int batchSize,
        @DefaultValue("16777216") long batchBytes,
        @DefaultValue("130s") Duration batchTimeout,
        @DefaultValue("30s") Duration shutdownTimeout,
        @DefaultValue Retry retry,
        @DefaultValue Requeue requeue
    ) implements RelayConfig {}

    /**
     * The backoff of failed records.
     *
     * @param initialDelay the first backoff of a head in the retry table and of a shard's pause, doubled with each
     *     failure. Default 1 s.
     * @param maxDelay the backoff's cap. Default 5 min.
     * @param maxAttempts a record that failed this many attempts moves to the dead table; 0 = retry forever, the
     *     default.
     * @param holdPermanent a Permanent outcome stops its (topic, key) as a Retry does instead of moving the record to
     *     the dead table while its key goes on: the record stays the retry head, spends attempts and shows in
     *     outbox.retry.oldest. A key is never skipped past its record only with maxAttempts = 0. Default false.
     */
    public record Retry(
        @DefaultValue("1s") Duration initialDelay,
        @DefaultValue("5m") Duration maxDelay,
        @DefaultValue("0") int maxAttempts,
        @DefaultValue("false") boolean holdPermanent
    ) implements RetryConfig {}

    /**
     * Records the handler set aside with Outcome.requeue().
     *
     * @param maxRequeues requeues a record may take; the one past it moves the record to the dead table. 0 = unlimited,
     *     the default.
     */
    public record Requeue(
        @DefaultValue("0") int maxRequeues
    ) implements RequeueConfig {}

    /**
     * The outbox table's partitions.
     *
     * @param tailConversion when a hot tail over the swap's scan limit (16 MB) is turned into a range all the same:
     *     writers wait for its scan.
     */
    public record Partitions(
        @DefaultValue TailConversion tailConversion
    ) implements PartitionsConfig {}

    /**
     * When a hot tail over the swap's scan limit (16 MB) is turned into a range all the same: writers wait for its
     * scan.
     *
     * @param window HH:mm-HH:mm in zone, crossing midnight when the end comes first; empty: never. Default
     *     03:00-05:00.
     * @param maxStall the scan's statement_timeout in the window, so about the longest writers wait for its hold: keep
     *     it below their own lock_timeout and statement_timeout. A scan that does not fit rolls back, nothing left
     *     behind, and this instance tries again in the next window. Default 1 s, at most 24 days.
     * @param zone the IANA time zone window is read in. Default: the JVM's default zone.
     */
    public record TailConversion(
        @DefaultValue("03:00-05:00") String window,
        @DefaultValue("1s") Duration maxStall,
        @Nullable String zone
    ) implements TailConversionConfig {

        public TailConversion {
            if (zone == null) {
                zone = ZoneId.systemDefault().getId();
            }
        }
    }

    /**
     * The inbox table that deduplicates handled messages.
     *
     * @param table the inbox table's name, same rules as outbox.table; created only when a component uses Inbox.
     *     Default inbox.
     * @param retention ranges older than this are dropped; ids up to retention - 1 h (one inbox range) old are always
     *     deduplicated. At least 2 h. Default 24 h.
     */
    public record Inbox(
        @DefaultValue("inbox") String table,
        @DefaultValue("24h") Duration retention
    ) implements InboxConfig {}

    /**
     * Records moved to the dead table and the dead-letter hook.
     *
     * @param retention kept letters older than this, by their failed_at, are deleted by maintenance; 0 = kept forever,
     *     the default; at most 3650 days. Letters still pending for the hook are never deleted. Deleted in portions,
     *     a maintenance run spending at most its 30 s interval on them with the redrive history's cleanup; later runs
     *     finish the rest.
     */
    public record DeadLetters(
        @DefaultValue("0s") Duration retention
    ) implements DeadLettersConfig {}

    /**
     * What happens when the xid counter is found below a tick's xmax (a logical restore or migration).
     *
     * @param timeout the whole recovery transaction's deadline, writers waiting for it all along; each statement gets
     *     what is left. Default 10 min, at most 24 days.
     */
    public record Recovery(
        @DefaultValue("10m") Duration timeout
    ) implements RecoveryConfig {}

    /**
     * The outbox's base tables.
     *
     * @param bootstrap true (default): every instance creates and upgrades the base tables at startup. False: the
     *     application's migrations own them (OutboxSql), and startup only checks them and the roles' rights, failing
     *     with every problem named; partitions are the library's in both modes.
     */
    public record Schema(
        @DefaultValue("true") boolean bootstrap
    ) implements SchemaConfig {}

    /**
     * The JSON mapper of typed payloads.
     *
     * @param mapper auto (default): Jackson 2 when its ObjectMapper bean exists, else Jackson 3's JsonMapper;
     *     jackson2 or jackson3: that one, the start failing when it is absent.
     */
    public record Json(
        @DefaultValue("auto") Mapper mapper
    ) {

        /** Which Jackson maps typed payloads. */
        public enum Mapper { AUTO, JACKSON2, JACKSON3 }
    }

    /**
     * The connections of the heartbeat, which keeps this instance's row fresh and reads the reports that guard routed
     * sends.
     *
     * @param dedicatedConnection whether the heartbeat gets a pool of its own, so that business load that exhausts the
     *     app's pool does not starve its beats: one connection, always open, copied from the HikariDataSource the
     *     outbox's DataSource is or unwraps to, with the same database, role, driver properties and meters, named
     *     after that pool with -outbox-heartbeat appended (outbox-heartbeat while it has no name), its connection and
     *     validation timeouts capped by the 30 s maintenance interval. Count its connection into
     *     max_connections, the role's CONNECTION LIMIT and a pooler's per-user pool. A DataSource bean qualified
     *     @OutboxHeartbeat wins over this key; false keeps the heartbeat on the app's connections. Default true.
     */
    public record Heartbeat(
        @DefaultValue("true") boolean dedicatedConnection
    ) {}
}
