package io.github.dsudomoin.outbox.kora;

import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.ConfigFactory;
import io.github.dsudomoin.outbox.JdbcTransactions;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import ru.tinkoff.kora.config.common.origin.SimpleConfigOrigin;
import ru.tinkoff.kora.config.hocon.HoconConfigFactory;
import ru.tinkoff.kora.database.common.telemetry.DataBaseTelemetry;
import ru.tinkoff.kora.database.jdbc.JdbcConnectionFactory;
import ru.tinkoff.kora.database.jdbc.JdbcHelper;

/** The heartbeat pool's config, copied from plain Hikari configs: no pool, no database. */
class HeartbeatPoolTest {

    @Test
    void deriveSkipsWhatItCannotCopy() {
        var interval = Duration.ofSeconds(30);

        assertThat(HeartbeatPool.derive(new JdbcTransactions(new PGSimpleDataSource()), interval))
                .as("not KoraOutboxTransactions").isNull();
        assertThat(HeartbeatPool.derive(new KoraOutboxTransactions(new JdbcConnectionFactory() {
            @Override
            public <T> T withConnection(JdbcHelper.SqlFunction1<Connection, T> callback) {
                throw new UnsupportedOperationException();
            }

            @Nullable
            @Override
            public Connection currentConnection() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Connection newConnection() {
                throw new UnsupportedOperationException();
            }

            @Override
            public DataBaseTelemetry telemetry() {
                throw new UnsupportedOperationException();
            }
        }), interval)).as("over a factory that is not a JdbcDatabase").isNull();
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
