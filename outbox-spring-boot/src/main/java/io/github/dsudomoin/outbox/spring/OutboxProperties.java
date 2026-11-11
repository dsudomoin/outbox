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
 * @param shards not set (the default): an existing outbox keeps the shard count of its cursors, a new one gets 256.
 *     A set value that differs from the cursors' reshards at bootstrap, which refuses unless the outbox is drained and
 *     no other instance runs.
 * @param networkTimeout how long the library's own connections wait on a silent socket, when they have no such bound
 *     of their own (pgjdbc's default socketTimeout = 0): the start, relay workers, drain(), maintenance, the
 *     dead-letter pass, the heartbeat (at most its interval, at least 10 s, even under a longer socketTimeout) and
 *     admin calls. 0: no bound from here. Default 5 min.
 * @param relay the relay that hands the outbox's records to the handlers.
 * @param partitions the outbox table's partitions.
 * @param inbox the inbox table that deduplicates handled messages.
 * @param deadLetters records moved to the dead table and the dead-letter hook.
 * @param recovery what happens when the xid counter is found below a tick's xmax (a logical restore or migration).
 * @param schema the outbox's base tables.
 * @param enabled false: no outbox runtime, the inbox only. Default true.
 * @param lifecycle the outbox's start and stop within the application context.
 * @param telemetry the outbox's observations.
 * @param json the JSON mapper of typed payloads.
 * @param heartbeat the connections of the heartbeat, which keeps this instance's row fresh and reads the reports that
 *     guard routed sends.
 */
@ConfigurationProperties("outbox")
public record OutboxProperties(
    @DefaultValue("outbox") String table,
    @Nullable Integer shards,
    @DefaultValue("5m") Duration networkTimeout,
    @DefaultValue Relay relay,
    @DefaultValue Partitions partitions,
    @DefaultValue Inbox inbox,
    @DefaultValue DeadLetters deadLetters,
    @DefaultValue Recovery recovery,
    @DefaultValue Schema schema,
    @DefaultValue("true") boolean enabled,
    @DefaultValue Lifecycle lifecycle,
    @DefaultValue Telemetry telemetry,
    @DefaultValue Json json,
    @DefaultValue Heartbeat heartbeat
) implements OutboxConfig {

    /**
     * The relay that hands the outbox's records to the handlers.
     *
     * @param enabled false: this instance runs no relay workers, dead-letter pass and partition maintenance;
     *     OutboxRelay.drain() and the instance's heartbeat still work. Keep at least one instance with the relay on.
     *     Default true.
     * @param workers relay threads per instance (outbox-relay-N), at least 1; each holds a database connection while it
     *     works. Default 4.
     * @param batchSize rows per chunk read and per handler call. Keep batchSize × the handler's time per record well
     *     below batch-timeout: a longer call is cut at its soft deadline and the shard moves on only by the records the
     *     handler finished. Default 1000.
     * @param maxTicksPerBatch the most ticks one batch may span. Default 100.
     * @param minTickInterval under load, ticks come at most once per this interval; a commit waits for its tick up to
     *     about twice it. Default 50 ms.
     * @param idlePollInterval how often idle ticks come (about one per interval for the whole cluster), and how soon a
     *     commit on an instance without a local wake-up is noticed. Default 200 ms.
     * @param batchTimeout bound of one handler call. At 80 % of it the handler thread is interrupted and the call's
     *     real outcomes are kept (records it did not start are read again); a call still running at the full timeout
     *     is cancelled and all its records are retried, delivered ones included. Per-record timeouts of the handler
     *     (HTTP read timeouts, each byTopic route's own) must be well below it. Default 30 s, at most 24 days.
     * @param shutdownTimeout on stop, unfinished batches get this long, then their workers are interrupted and the
     *     batches are delivered again later. Default 30 s.
     * @param connectionHold how long a relay worker starts steps on one pooled connection under a backlog before it
     *     hands it back and takes another, so the pool can retire it at its max-lifetime. A step started just before
     *     this ends runs on the same connection, up to 4 × batch-timeout: a pool's leak-detection-threshold must
     *     exceed connection-hold + 4 × batch-timeout. Default 1 min.
     * @param headFailuresBeforeDivert failed attempts in a row after which a shard reads one head row and diverts the
     *     single failing key. Default 3.
     * @param cursorAnchorInterval how long an idle shard's latest cursor row may stay on a tick below its effective
     *     one before maintenance appends one at the effective tick (an anchor): ticks are kept from the lowest latest
     *     cursor row's tick on, as the previous release starts its steps there. At least
     *     outbox.partitions.maintenance-interval. Default 1 h.
     * @param retry the backoff of failed records.
     * @param requeue records the handler set aside with Outcome.requeue().
     * @param waiting records waiting for an instance that can handle them.
     */
    public record Relay(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("4") int workers,
        @DefaultValue("1000") int batchSize,
        @DefaultValue("100") int maxTicksPerBatch,
        @DefaultValue("50ms") Duration minTickInterval,
        @DefaultValue("200ms") Duration idlePollInterval,
        @DefaultValue("30s") Duration batchTimeout,
        @DefaultValue("30s") Duration shutdownTimeout,
        @DefaultValue("1m") Duration connectionHold,
        @DefaultValue("3") int headFailuresBeforeDivert,
        @DefaultValue("1h") Duration cursorAnchorInterval,
        @DefaultValue Retry retry,
        @DefaultValue Requeue requeue,
        @DefaultValue Waiting waiting
    ) implements RelayConfig {}

    /**
     * The backoff of failed records.
     *
     * @param initialDelay the first backoff of a head in the retry table and of a shard's pause, doubled with each
     *     failure. Default 1 s.
     * @param maxDelay the backoff's cap. Default 5 min.
     * @param maxAttempts a record that failed this many attempts moves to the dead table; 0 = retry forever, the
     *     default.
     */
    public record Retry(
        @DefaultValue("1s") Duration initialDelay,
        @DefaultValue("5m") Duration maxDelay,
        @DefaultValue("0") int maxAttempts
    ) implements RetryConfig {}

    /**
     * Records the handler set aside with Outcome.requeue().
     *
     * @param delay a requeued record is due delay later, or at the handler's later notBefore. Default 1 s.
     * @param maxRequeues requeues a record may take; the one past it moves the record to the dead table. 0 = unlimited,
     *     the default.
     */
    public record Requeue(
        @DefaultValue("1s") Duration delay,
        @DefaultValue("0") int maxRequeues
    ) implements RequeueConfig {}

    /**
     * Records waiting for an instance that can handle them (Outcome.Waiting): each is handed again after a quarter of
     * the time it has waited, at least delay and at most max-delay later.
     *
     * @param delay the shortest time until a waiting head is handed again. Default 1 s.
     * @param maxDelay the longest time until a waiting head is handed again. Default 5 min.
     * @param alertAfter a record waiting longer turns the periodic WARN about waiting records into an ERROR. Default
     *     15 min.
     */
    public record Waiting(
        @DefaultValue("1s") Duration delay,
        @DefaultValue("5m") Duration maxDelay,
        @DefaultValue("15m") Duration alertAfter
    ) implements WaitConfig {}

    /**
     * The outbox table's partitions.
     *
     * @param xidsPerPartition the size of one xid range partition (2^24 xids). Default 16777216.
     * @param premake ranges kept above the current xid; maintenance extends them when half is used. Default 64.
     * @param maintenanceInterval the period of the outbox's and the inbox's maintenance and of each instance's
     *     heartbeat. Default 30 s.
     * @param swapLockTimeout how long the tail swap waits in the queue for its lock on the outbox once its tries
     *     without waiting failed, writers queueing behind it meanwhile: keep it below their own lock_timeout and
     *     statement_timeout. 0 (default): no wait, so writers wait only for the swap's hold. At most 24 days, with
     *     tailConversion.maxStall added too.
     * @param maxTailScan largest tail, in bytes (pg_relation_size), that the swap turns into a range when rows went to
     *     it, which scans it under the swap's lock: writers wait for the scan. A larger tail stays hot, writes going on
     *     into it, until tail-conversion's window. Default 16 MB.
     * @param restoreOrphans move rows at or above the horizon in a range marked for drop back to the outbox; false only
     *     logs. Default true.
     * @param tailConversion when a hot tail over max-tail-scan is turned into a range all the same: writers wait for
     *     its scan.
     */
    public record Partitions(
        @DefaultValue("16777216") long xidsPerPartition,
        @DefaultValue("64") int premake,
        @DefaultValue("30s") Duration maintenanceInterval,
        @DefaultValue("0s") Duration swapLockTimeout,
        @DefaultValue("16777216") long maxTailScan,
        @DefaultValue("true") boolean restoreOrphans,
        @DefaultValue TailConversion tailConversion
    ) implements PartitionsConfig {}

    /**
     * When a hot tail over max-tail-scan is turned into a range all the same: writers wait for its scan.
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
     * @param partitionInterval the time span of one inbox range (by UUIDv7 time), a whole number of minutes, at
     *     least 1 m. Default 1 h.
     * @param premake inbox ranges are kept up to now + premake. Default 24 h.
     * @param retention ranges older than this are dropped; ids up to retention - partition-interval old are always
     *     deduplicated. Default 24 h.
     * @param attachOrphans attach a range detached inside the dedup window back by its name's bounds; false only logs.
     *     Default true.
     */
    public record Inbox(
        @DefaultValue("inbox") String table,
        @DefaultValue("1h") Duration partitionInterval,
        @DefaultValue("24h") Duration premake,
        @DefaultValue("24h") Duration retention,
        @DefaultValue("true") boolean attachOrphans
    ) implements InboxConfig {}

    /**
     * Records moved to the dead table and the dead-letter hook.
     *
     * @param batchSize letters per call of the dead-letter hook, 1..1000 (a redrive batch holds a key lock each).
     *     Default 100.
     * @param timeout bound of one hook call; a call cut at it leaves its letters pending, retried with backoff. Default
     *     30 s, at most 24 days.
     * @param retention kept letters older than this, by their failed_at, are deleted by maintenance; 0 = kept forever,
     *     the default; at most 3650 days. Letters still pending for the hook are never deleted. Deleted in portions,
     *     at most a maintenanceInterval per run with the redrive history's cleanup; later runs finish the rest.
     * @param redriveHistory how long the count of a message's redrives is remembered, so a redriven message that dies
     *     again carries it. Default 30 days, at most 3650 days.
     */
    public record DeadLetters(
        @DefaultValue("100") int batchSize,
        @DefaultValue("30s") Duration timeout,
        @DefaultValue("0s") Duration retention,
        @DefaultValue("30d") Duration redriveHistory
    ) implements DeadLettersConfig {}

    /**
     * What happens when the xid counter is found below a tick's xmax (a logical restore or migration).
     *
     * @param xidRegression auto (default): the instance holding the maintenance lock recovers by itself, when at most
     *     max-rows rows are to be re-sent; halt: never by itself, the relay stays halted until
     *     OutboxAdmin.recoverXidRegression().
     * @param maxRows rows to re-send above which the automatic recovery leaves the relay halted: writers wait while it
     *     copies them. Default 1 000 000.
     * @param clockMargin rows written since the newest pre-regression tick, less this margin for clock skew between the
     *     old and the new server, are re-sent. Default 10 min.
     * @param timeout the whole recovery transaction's deadline, writers waiting for it all along; each statement gets
     *     what is left. Default 10 min, at most 24 days.
     */
    public record Recovery(
        @DefaultValue("auto") String xidRegression,
        @DefaultValue("1000000") long maxRows,
        @DefaultValue("10m") Duration clockMargin,
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
     * The outbox's start and stop within the application context.
     *
     * @param phase the outbox's SmartLifecycle phase. Default Integer.MAX_VALUE - 4096 (2147479551), below the web
     *     server's, the Kafka listener containers' and the default one: the outbox starts before them and stops after.
     */
    public record Lifecycle(
        @DefaultValue("2147479551") int phase
    ) {}

    /**
     * The outbox's observations.
     *
     * @param observeUnsampled whether to observe the delivery of a record whose stored trace is not sampled; false by
     *     default, so the delivery follows the stored trace's sampling decision, as a parent-based sampler would, and
     *     true gives every delivered record an outbox.deliver observation.
     */
    public record Telemetry(
        @DefaultValue("false") boolean observeUnsampled
    ) {}

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
     *     validation timeouts capped by outbox.partitions.maintenance-interval. Count its connection into
     *     max_connections, the role's CONNECTION LIMIT and a pooler's per-user pool. A DataSource bean qualified
     *     @OutboxHeartbeat wins over this key; false keeps the heartbeat on the app's connections. Default true.
     */
    public record Heartbeat(
        @DefaultValue("true") boolean dedicatedConnection
    ) {}
}
