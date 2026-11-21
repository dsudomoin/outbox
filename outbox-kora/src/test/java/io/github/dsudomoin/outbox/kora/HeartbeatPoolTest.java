package io.github.dsudomoin.outbox.kora;

import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.ConfigFactory;
import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.testing.TestDb;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import ru.tinkoff.kora.config.common.origin.SimpleConfigOrigin;
import ru.tinkoff.kora.config.hocon.HoconConfigFactory;

/** What the Kora adapter adds to the heartbeat pool: when it derives one, and the HOCON flag; no database. */
class HeartbeatPoolTest {

    @Test
    void deriveSkipsWhatItCannotCopy() {
        var interval = Duration.ofSeconds(30);

        assertThat(HeartbeatPool.derive(new JdbcTransactions(new PGSimpleDataSource()), interval))
                .as("not KoraOutboxTransactions").isNull();
        assertThat(HeartbeatPool.derive(new KoraOutboxTransactions(TestDb.opening(null, () -> null)), interval))
                .as("over a factory that is not a JdbcDatabase").isNull();
    }

    @Test
    void theFlagIsReadAtOutboxHeartbeatAndOnByDefault() {
        assertThat(flag("").dedicatedConnection()).isTrue();
        assertThat(flag("outbox { table = t }").dedicatedConnection()).isTrue();
        assertThat(flag("outbox.heartbeat.dedicatedConnection = false").dedicatedConnection()).isFalse();
    }

    private static OutboxHeartbeatConfig flag(String hocon) {
        var config = HoconConfigFactory.fromHocon(
                new SimpleConfigOrigin("test"), ConfigFactory.parseString(hocon).resolve());
        return new OutboxModule() {}.outboxHeartbeatConfig(config, new $OutboxHeartbeatConfig_ConfigValueExtractor());
    }
}
