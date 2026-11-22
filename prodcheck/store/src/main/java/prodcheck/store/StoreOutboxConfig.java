package prodcheck.store;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.internal.Tuning;
import io.github.dsudomoin.outbox.spring.OutboxProperties;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.boot.context.properties.bind.Binder;

/**
 * The app's own outbox config, so the scenarios set the library's internal {@link Tuning} as they do on the Kora apps:
 * {@code outbox.*} as the auto-configuration binds it, the tuning from the keys right under {@code outbox}
 * ({@code outbox.maintenance-interval}, {@code -Doutbox.maintenanceInterval=2s}), each read once.
 */
final class StoreOutboxConfig implements OutboxConfig, Tuning {

    private final OutboxProperties properties;
    private final Binder binder;
    private final Map<String, Object> tuning = new ConcurrentHashMap<>();

    StoreOutboxConfig(Binder binder) {
        this.properties = binder.bindOrCreate("outbox", OutboxProperties.class);
        this.binder = binder;
    }

    private <T> T tuned(String key, Class<T> type, T otherwise) {
        return type.cast(tuning.computeIfAbsent(key, k -> binder.bind("outbox." + k, type).orElse(otherwise)));
    }

    @Override
    public String table() {
        return properties.table();
    }

    @Override
    public RelayConfig relay() {
        return properties.relay();
    }

    @Override
    public PartitionsConfig partitions() {
        return properties.partitions();
    }

    @Override
    public InboxConfig inbox() {
        return properties.inbox();
    }

    @Override
    public DeadLettersConfig deadLetters() {
        return properties.deadLetters();
    }

    @Override
    public RecoveryConfig recovery() {
        return properties.recovery();
    }

    @Override
    public SchemaConfig schema() {
        return properties.schema();
    }

    @Override
    public int shards() {
        return tuned("shards", Integer.class, Tuning.super.shards());
    }

    @Override
    public Duration networkTimeout() {
        return tuned("network-timeout", Duration.class, Tuning.super.networkTimeout());
    }

    @Override
    public int maxTicksPerBatch() {
        return tuned("max-ticks-per-batch", Integer.class, Tuning.super.maxTicksPerBatch());
    }

    @Override
    public Duration minTickInterval() {
        return tuned("min-tick-interval", Duration.class, Tuning.super.minTickInterval());
    }

    @Override
    public Duration idlePollInterval() {
        return tuned("idle-poll-interval", Duration.class, Tuning.super.idlePollInterval());
    }

    @Override
    public Duration connectionHold() {
        return tuned("connection-hold", Duration.class, Tuning.super.connectionHold());
    }

    @Override
    public Duration ticksWithoutRelay() {
        return tuned("ticks-without-relay", Duration.class, Tuning.super.ticksWithoutRelay());
    }

    @Override
    public int headFailuresBeforeDivert() {
        return tuned("head-failures-before-divert", Integer.class, Tuning.super.headFailuresBeforeDivert());
    }

    @Override
    public Duration cursorAnchorInterval() {
        return tuned("cursor-anchor-interval", Duration.class, Tuning.super.cursorAnchorInterval());
    }

    @Override
    public Duration requeueDelay() {
        return tuned("requeue-delay", Duration.class, Tuning.super.requeueDelay());
    }

    @Override
    public Duration waitDelay() {
        return tuned("wait-delay", Duration.class, Tuning.super.waitDelay());
    }

    @Override
    public Duration waitMaxDelay() {
        return tuned("wait-max-delay", Duration.class, Tuning.super.waitMaxDelay());
    }

    @Override
    public Duration waitAlertAfter() {
        return tuned("wait-alert-after", Duration.class, Tuning.super.waitAlertAfter());
    }

    @Override
    public long xidsPerPartition() {
        return tuned("xids-per-partition", Long.class, Tuning.super.xidsPerPartition());
    }

    @Override
    public int premake() {
        return tuned("premake", Integer.class, Tuning.super.premake());
    }

    @Override
    public Duration maintenanceInterval() {
        return tuned("maintenance-interval", Duration.class, Tuning.super.maintenanceInterval());
    }

    @Override
    public Duration swapLockTimeout() {
        return tuned("swap-lock-timeout", Duration.class, Tuning.super.swapLockTimeout());
    }

    @Override
    public long maxTailScan() {
        return tuned("max-tail-scan", Long.class, Tuning.super.maxTailScan());
    }

    @Override
    public Duration inboxPartitionInterval() {
        return tuned("inbox-partition-interval", Duration.class, Tuning.super.inboxPartitionInterval());
    }

    @Override
    public Duration inboxPremake() {
        return tuned("inbox-premake", Duration.class, Tuning.super.inboxPremake());
    }

    @Override
    public int deadLetterBatchSize() {
        return tuned("dead-letter-batch-size", Integer.class, Tuning.super.deadLetterBatchSize());
    }

    @Override
    public Duration deadLetterTimeout() {
        return tuned("dead-letter-timeout", Duration.class, Tuning.super.deadLetterTimeout());
    }

    @Override
    public Duration redriveHistory() {
        return tuned("redrive-history", Duration.class, Tuning.super.redriveHistory());
    }

    @Override
    public String xidRegression() {
        return tuned("xid-regression", String.class, Tuning.super.xidRegression());
    }

    @Override
    public long recoveryMaxRows() {
        return tuned("recovery-max-rows", Long.class, Tuning.super.recoveryMaxRows());
    }

    @Override
    public Duration recoveryClockMargin() {
        return tuned("recovery-clock-margin", Duration.class, Tuning.super.recoveryClockMargin());
    }
}
