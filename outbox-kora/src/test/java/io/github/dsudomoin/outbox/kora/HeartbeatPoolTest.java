package io.github.dsudomoin.outbox.kora;

import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.ConfigFactory;
import com.zaxxer.hikari.HikariConfig;
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
    void copiesTheAppPoolAndPinsItsShape() {
        var config = HeartbeatPool.config(app(), Duration.ofSeconds(30));

        assertThat(config.getMaximumPoolSize()).isEqualTo(1);
        assertThat(config.getMinimumIdle()).isEqualTo(1);
        assertThat(config.getIdleTimeout()).isZero();
        assertThat(config.getInitializationFailTimeout()).isEqualTo(-1);
        assertThat(config.getPoolName()).isEqualTo("app-outbox-heartbeat");
        assertThat(config.getJdbcUrl()).isEqualTo("jdbc:postgresql://h/db");
        assertThat(config.getUsername()).isEqualTo("u");
        assertThat(config.getPassword()).isEqualTo("p");
        assertThat(config.getDataSourceProperties().getProperty("ssl")).isEqualTo("true");
        assertThat(config.getConnectionTimeout()).isEqualTo(7_500);
        assertThat(config.getValidationTimeout()).isEqualTo(3_750);
    }

    @Test
    void floorsHoldAtShortIntervals() {
        for (var interval : new Duration[] {Duration.ofSeconds(1), Duration.ofMillis(200)}) {
            var config = HeartbeatPool.config(app(), interval);

            assertThat(config.getConnectionTimeout()).as("at %s", interval).isEqualTo(500);
            assertThat(config.getValidationTimeout()).as("at %s", interval).isEqualTo(250);
        }
    }

    @Test
    void smallerCopiedTimeoutsStand() {
        var shortConnect = app();
        shortConnect.setConnectionTimeout(2_000);
        var config = HeartbeatPool.config(shortConnect, Duration.ofSeconds(30));
        assertThat(config.getConnectionTimeout()).isEqualTo(2_000);
        assertThat(config.getValidationTimeout()).isEqualTo(1_000);

        var shortValidation = app();
        shortValidation.setValidationTimeout(1_000);
        config = HeartbeatPool.config(shortValidation, Duration.ofSeconds(10));
        assertThat(config.getConnectionTimeout()).isEqualTo(2_500);
        assertThat(config.getValidationTimeout()).isEqualTo(1_000);
    }

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

    private static HikariConfig app() {
        var app = new HikariConfig();
        app.setPoolName("app");
        app.setJdbcUrl("jdbc:postgresql://h/db");
        app.setUsername("u");
        app.setPassword("p");
        app.addDataSourceProperty("ssl", "true");
        app.setMaximumPoolSize(10);
        app.setMinimumIdle(0);
        app.setConnectionTimeout(10_000);
        app.setValidationTimeout(5_000);
        return app;
    }
}
