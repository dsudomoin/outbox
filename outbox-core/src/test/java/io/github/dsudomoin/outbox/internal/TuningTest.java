package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** Tuning's defaults are the production values: a change here changes every deployment. */
class TuningTest {

    @Test
    void defaultsAreTheProductionValues() {
        var t = Tuning.of(new OutboxConfig() {});

        assertThat(t).isSameAs(Tuning.DEFAULTS);
        assertThat(t.shards()).isEqualTo(256);
        assertThat(t.networkTimeout()).isEqualTo(Duration.ofMinutes(5));
        assertThat(t.maxTicksPerBatch()).isEqualTo(100);
        assertThat(t.minTickInterval()).isEqualTo(Duration.ofMillis(50));
        assertThat(t.idlePollInterval()).isEqualTo(Duration.ofMillis(200));
        assertThat(t.connectionHold()).isEqualTo(Duration.ofMinutes(1));
        assertThat(t.ticksWithoutRelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(t.headFailuresBeforeDivert()).isEqualTo(3);
        assertThat(t.cursorAnchorInterval()).isEqualTo(Duration.ofHours(1));
        assertThat(t.requeueDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(t.waitDelay()).isEqualTo(Duration.ofSeconds(1));
        assertThat(t.waitMaxDelay()).isEqualTo(Duration.ofMinutes(5));
        assertThat(t.waitAlertAfter()).isEqualTo(Duration.ofMinutes(15));
        assertThat(t.xidsPerPartition()).isEqualTo(16_777_216L);
        assertThat(t.premake()).isEqualTo(64);
        assertThat(t.maintenanceInterval()).isEqualTo(Duration.ofSeconds(30));
        assertThat(t.swapLockTimeout()).isZero();
        assertThat(t.maxTailScan()).isEqualTo(16L * 1024 * 1024);
        assertThat(t.inboxPartitionInterval()).isEqualTo(Duration.ofHours(1));
        assertThat(t.inboxPremake()).isEqualTo(Duration.ofHours(24));
        assertThat(t.deadLetterBatchSize()).isEqualTo(100);
        assertThat(t.deadLetterTimeout()).isEqualTo(Duration.ofSeconds(130));
        assertThat(t.redriveHistory()).isEqualTo(Duration.ofDays(30));
        assertThat(t.xidRegression()).isEqualTo("auto");
        assertThat(t.recoveryMaxRows()).isEqualTo(1_000_000L);
        assertThat(t.recoveryClockMargin()).isEqualTo(Duration.ofMinutes(10));
    }

    @Test
    void aConfigThatImplementsTuningKeepsItsOwnThroughTheResolvedWrapper() {
        var own = new TunedConfig();

        assertThat(Tuning.of(own)).isSameAs(own);
        assertThat(Tuning.of(new OutboxSchema.Resolved(own, 4, false))).isSameAs(own);
    }

    private static final class TunedConfig implements OutboxConfig, Tuning {}
}
