package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.internal.Tuning;
import java.time.Duration;

/**
 * Mutable config with fast test defaults and fresh {@link TestSql#uniqueTable()} names for both tables, and the
 * library's {@link Tuning} too. Relay, retry, requeue, partition, tail conversion (its window off by default) and
 * recovery settings live on this object itself ({@link #relay()}, {@link #retry()}, {@link #requeue()},
 * {@link #partitions()}, {@link #tailConversion()} and {@link #recovery()} return {@code this}); the inbox and
 * dead-letter blocks are separate because their {@code table()} and {@code retention()} clash with other ones.
 */
public final class TestOutboxConfig implements OutboxConfig, OutboxConfig.RelayConfig, OutboxConfig.RetryConfig,
        OutboxConfig.RequeueConfig, OutboxConfig.PartitionsConfig, OutboxConfig.TailConversionConfig,
        OutboxConfig.RecoveryConfig, Tuning {

    private String table = TestSql.uniqueTable();
    private int shards = 4;
    private boolean relayEnabled = true;
    private int workers = 2;
    private int batchSize = 100;
    private long batchBytes = new RelayConfig() {}.batchBytes();
    private int maxTicksPerBatch = 100;
    private Duration minTickInterval = Duration.ofMillis(5);
    private Duration idlePollInterval = Duration.ofMillis(20);
    private Duration batchTimeout = Duration.ofSeconds(5);
    private Duration shutdownTimeout = Duration.ofSeconds(5);
    private Duration connectionHold = Duration.ofMinutes(1);
    /** Off: the tests that run with the relay disabled make their ticks themselves, at the moments they choose. */
    private Duration ticksWithoutRelay = Duration.ZERO;
    private Duration networkTimeout = Duration.ofMinutes(5);
    private int headFailuresBeforeDivert = 3;
    private Duration cursorAnchorInterval = Duration.ofHours(1);
    private Duration retryInitial = Duration.ofMillis(50);
    private Duration retryMax = Duration.ofMillis(500);
    private int retryMaxAttempts = 0;
    private boolean holdPermanent = false;
    private Duration requeueDelay = Duration.ofMillis(100);
    private int maxRequeues = 0;
    private Duration waitDelay = Duration.ofMillis(50);
    private Duration waitMax = Duration.ofMillis(500);
    private Duration waitAlertAfter = Duration.ofMinutes(15);
    private long xidsPerPartition = 1_000_000L;
    private int premake = 4;
    private Duration maintenanceInterval = Duration.ofHours(1); // tests call maintenance directly
    private Duration swapLockTimeout = Duration.ZERO;
    private long maxTailScan = Tuning.DEFAULTS.maxTailScan();
    private String tailConversionWindow = ""; // off, whatever time the test runs at
    private Duration tailConversionMaxStall = Duration.ofSeconds(1);
    private String tailConversionZone = java.time.ZoneId.systemDefault().getId();
    private String inboxTable = TestSql.uniqueTable();
    private Duration inboxPartitionInterval = Duration.ofHours(1);
    private Duration inboxPremake = Duration.ofHours(24);
    private Duration inboxRetention = Duration.ofHours(24);
    private int deadLettersBatchSize = 100;
    private Duration deadLettersTimeout = Duration.ofSeconds(5);
    private Duration deadLettersRetention = Duration.ZERO;
    private Duration deadLettersRedriveHistory = Duration.ofDays(30);
    private String xidRegression = "auto";
    private long maxRows = 1_000_000L;
    private Duration clockMargin = Duration.ofMinutes(10);
    private Duration recoveryTimeout = Duration.ofMinutes(10);
    private boolean schemaBootstrap = true;

    // OutboxConfig

    @Override
    public String table() {
        return table;
    }

    public TestOutboxConfig table(String table) {
        this.table = table;
        return this;
    }

    @Override
    public int shards() {
        return shards;
    }

    /** A new outbox's shard count; an existing one keeps its cursors'. */
    public TestOutboxConfig shards(int shards) {
        this.shards = shards;
        return this;
    }

    @Override
    public RelayConfig relay() {
        return this;
    }

    @Override
    public PartitionsConfig partitions() {
        return this;
    }

    @Override
    public InboxConfig inbox() {
        return new InboxConfig() {
            @Override
            public String table() {
                return inboxTable;
            }

            @Override
            public Duration retention() {
                return inboxRetention;
            }
        };
    }

    @Override
    public DeadLettersConfig deadLetters() {
        return new DeadLettersConfig() {
            @Override
            public Duration retention() {
                return deadLettersRetention;
            }
        };
    }

    @Override
    public int deadLetterBatchSize() {
        return deadLettersBatchSize;
    }

    @Override
    public Duration deadLetterTimeout() {
        return deadLettersTimeout;
    }

    @Override
    public Duration redriveHistory() {
        return deadLettersRedriveHistory;
    }

    @Override
    public Duration inboxPartitionInterval() {
        return inboxPartitionInterval;
    }

    @Override
    public Duration inboxPremake() {
        return inboxPremake;
    }

    @Override
    public RecoveryConfig recovery() {
        return this;
    }

    @Override
    public SchemaConfig schema() {
        return new SchemaConfig() {
            @Override
            public boolean bootstrap() {
                return schemaBootstrap;
            }
        };
    }

    public TestOutboxConfig schemaBootstrap(boolean schemaBootstrap) {
        this.schemaBootstrap = schemaBootstrap;
        return this;
    }

    // RelayConfig

    @Override
    public boolean enabled() {
        return relayEnabled;
    }

    public TestOutboxConfig relayEnabled(boolean relayEnabled) {
        this.relayEnabled = relayEnabled;
        return this;
    }

    @Override
    public int workers() {
        return workers;
    }

    public TestOutboxConfig workers(int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public int batchSize() {
        return batchSize;
    }

    public TestOutboxConfig batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public long batchBytes() {
        return batchBytes;
    }

    public TestOutboxConfig batchBytes(long batchBytes) {
        this.batchBytes = batchBytes;
        return this;
    }

    @Override
    public int maxTicksPerBatch() {
        return maxTicksPerBatch;
    }

    public TestOutboxConfig maxTicksPerBatch(int maxTicksPerBatch) {
        this.maxTicksPerBatch = maxTicksPerBatch;
        return this;
    }

    @Override
    public Duration minTickInterval() {
        return minTickInterval;
    }

    public TestOutboxConfig minTickInterval(Duration minTickInterval) {
        this.minTickInterval = minTickInterval;
        return this;
    }

    @Override
    public Duration idlePollInterval() {
        return idlePollInterval;
    }

    public TestOutboxConfig idlePollInterval(Duration idlePollInterval) {
        this.idlePollInterval = idlePollInterval;
        return this;
    }

    @Override
    public Duration batchTimeout() {
        return batchTimeout;
    }

    public TestOutboxConfig batchTimeout(Duration batchTimeout) {
        this.batchTimeout = batchTimeout;
        return this;
    }

    @Override
    public Duration shutdownTimeout() {
        return shutdownTimeout;
    }

    public TestOutboxConfig shutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
        return this;
    }

    @Override
    public Duration connectionHold() {
        return connectionHold;
    }

    public TestOutboxConfig connectionHold(Duration connectionHold) {
        this.connectionHold = connectionHold;
        return this;
    }

    @Override
    public Duration ticksWithoutRelay() {
        return ticksWithoutRelay;
    }

    public TestOutboxConfig ticksWithoutRelay(Duration ticksWithoutRelay) {
        this.ticksWithoutRelay = ticksWithoutRelay;
        return this;
    }

    @Override
    public int headFailuresBeforeDivert() {
        return headFailuresBeforeDivert;
    }

    public TestOutboxConfig networkTimeout(Duration networkTimeout) {
        this.networkTimeout = networkTimeout;
        return this;
    }

    @Override
    public Duration networkTimeout() {
        return networkTimeout;
    }

    public TestOutboxConfig headFailuresBeforeDivert(int headFailuresBeforeDivert) {
        this.headFailuresBeforeDivert = headFailuresBeforeDivert;
        return this;
    }

    @Override
    public Duration cursorAnchorInterval() {
        return cursorAnchorInterval;
    }

    public TestOutboxConfig cursorAnchorInterval(Duration cursorAnchorInterval) {
        this.cursorAnchorInterval = cursorAnchorInterval;
        return this;
    }

    @Override
    public RetryConfig retry() {
        return this;
    }

    public TestOutboxConfig retry(Duration initialDelay, Duration maxDelay, int maxAttempts) {
        this.retryInitial = initialDelay;
        this.retryMax = maxDelay;
        this.retryMaxAttempts = maxAttempts;
        return this;
    }

    @Override
    public RequeueConfig requeue() {
        return this;
    }

    @Override
    public Duration waitDelay() {
        return waitDelay;
    }

    @Override
    public Duration waitMaxDelay() {
        return waitMax;
    }

    @Override
    public Duration waitAlertAfter() {
        return waitAlertAfter;
    }

    public TestOutboxConfig waiting(Duration delay, Duration maxDelay, Duration alertAfter) {
        this.waitDelay = delay;
        this.waitMax = maxDelay;
        this.waitAlertAfter = alertAfter;
        return this;
    }

    // RetryConfig

    @Override
    public Duration initialDelay() {
        return retryInitial;
    }

    @Override
    public Duration maxDelay() {
        return retryMax;
    }

    @Override
    public int maxAttempts() {
        return retryMaxAttempts;
    }

    @Override
    public boolean holdPermanent() {
        return holdPermanent;
    }

    public TestOutboxConfig holdPermanent(boolean holdPermanent) {
        this.holdPermanent = holdPermanent;
        return this;
    }

    // RequeueConfig

    @Override
    public Duration requeueDelay() {
        return requeueDelay;
    }

    public TestOutboxConfig requeueDelay(Duration requeueDelay) {
        this.requeueDelay = requeueDelay;
        return this;
    }

    @Override
    public int maxRequeues() {
        return maxRequeues;
    }

    public TestOutboxConfig maxRequeues(int maxRequeues) {
        this.maxRequeues = maxRequeues;
        return this;
    }

    // PartitionsConfig and the partitions' Tuning

    @Override
    public long xidsPerPartition() {
        return xidsPerPartition;
    }

    public TestOutboxConfig xidsPerPartition(long xidsPerPartition) {
        this.xidsPerPartition = xidsPerPartition;
        return this;
    }

    @Override
    public int premake() {
        return premake;
    }

    public TestOutboxConfig premake(int premake) {
        this.premake = premake;
        return this;
    }

    @Override
    public Duration maintenanceInterval() {
        return maintenanceInterval;
    }

    public TestOutboxConfig maintenanceInterval(Duration maintenanceInterval) {
        this.maintenanceInterval = maintenanceInterval;
        return this;
    }

    @Override
    public Duration swapLockTimeout() {
        return swapLockTimeout;
    }

    public TestOutboxConfig swapLockTimeout(Duration swapLockTimeout) {
        this.swapLockTimeout = swapLockTimeout;
        return this;
    }

    @Override
    public long maxTailScan() {
        return maxTailScan;
    }

    public TestOutboxConfig maxTailScan(long maxTailScan) {
        this.maxTailScan = maxTailScan;
        return this;
    }

    @Override
    public TailConversionConfig tailConversion() {
        return this;
    }

    @Override
    public String window() {
        return tailConversionWindow;
    }

    @Override
    public Duration maxStall() {
        return tailConversionMaxStall;
    }

    @Override
    public String zone() {
        return tailConversionZone;
    }

    public TestOutboxConfig tailConversionZone(String zone) {
        this.tailConversionZone = zone;
        return this;
    }

    public TestOutboxConfig tailConversion(String window, Duration maxStall) {
        this.tailConversionWindow = window;
        this.tailConversionMaxStall = maxStall;
        return this;
    }

    // InboxConfig

    /** This config on {@code other}'s outbox and inbox tables: a second instance of the same outbox. */
    public TestOutboxConfig sameTablesAs(TestOutboxConfig other) {
        return table(other.table()).inboxTable(other.inbox().table());
    }

    public TestOutboxConfig inboxTable(String inboxTable) {
        this.inboxTable = inboxTable;
        return this;
    }

    public TestOutboxConfig inboxPartitionInterval(Duration inboxPartitionInterval) {
        this.inboxPartitionInterval = inboxPartitionInterval;
        return this;
    }

    public TestOutboxConfig inboxPremake(Duration inboxPremake) {
        this.inboxPremake = inboxPremake;
        return this;
    }

    public TestOutboxConfig inboxRetention(Duration inboxRetention) {
        this.inboxRetention = inboxRetention;
        return this;
    }

    // DeadLettersConfig

    public TestOutboxConfig deadLettersBatchSize(int deadLettersBatchSize) {
        this.deadLettersBatchSize = deadLettersBatchSize;
        return this;
    }

    public TestOutboxConfig deadLettersTimeout(Duration deadLettersTimeout) {
        this.deadLettersTimeout = deadLettersTimeout;
        return this;
    }

    public TestOutboxConfig deadLettersRetention(Duration deadLettersRetention) {
        this.deadLettersRetention = deadLettersRetention;
        return this;
    }

    public TestOutboxConfig deadLettersRedriveHistory(Duration deadLettersRedriveHistory) {
        this.deadLettersRedriveHistory = deadLettersRedriveHistory;
        return this;
    }

    // RecoveryConfig and the recovery's Tuning

    @Override
    public String xidRegression() {
        return xidRegression;
    }

    public TestOutboxConfig xidRegression(String xidRegression) {
        this.xidRegression = xidRegression;
        return this;
    }

    @Override
    public long recoveryMaxRows() {
        return maxRows;
    }

    public TestOutboxConfig maxRows(long maxRows) {
        this.maxRows = maxRows;
        return this;
    }

    @Override
    public Duration recoveryClockMargin() {
        return clockMargin;
    }

    public TestOutboxConfig clockMargin(Duration clockMargin) {
        this.clockMargin = clockMargin;
        return this;
    }

    @Override
    public Duration timeout() {
        return recoveryTimeout;
    }

    public TestOutboxConfig recoveryTimeout(Duration recoveryTimeout) {
        this.recoveryTimeout = recoveryTimeout;
        return this;
    }
}
