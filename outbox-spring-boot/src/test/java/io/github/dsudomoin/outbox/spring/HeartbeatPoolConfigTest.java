package io.github.dsudomoin.outbox.spring;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import io.github.dsudomoin.outbox.JdbcTransactions;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy;

/** The heartbeat pool's config, copied from plain Hikari configs: no pool, no database. */
class HeartbeatPoolConfigTest {

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

    /** Boot names its pool only with spring.datasource.name; Hikari names one without a name when it starts. */
    @Test
    void anUnnamedAppPoolGivesTheBareName() {
        var unnamed = app();
        unnamed.setPoolName(null);

        assertThat(HeartbeatPool.config(unnamed, Duration.ofSeconds(30)).getPoolName()).isEqualTo("outbox-heartbeat");
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
    void deriveSkipsWhatItCannotCopy() throws Exception {
        var interval = Duration.ofSeconds(30);

        assertThat(HeartbeatPool.derive(new JdbcTransactions(new PGSimpleDataSource()), interval))
            .as("not SpringOutboxTransactions").isNull();
        assertThat(HeartbeatPool.derive(new SpringOutboxTransactions(new PGSimpleDataSource()), interval))
            .as("over a DataSource that is no HikariDataSource").isNull();
        assertThat(HeartbeatPool.derive(new SpringOutboxTransactions(
            new LazyConnectionDataSourceProxy(new PGSimpleDataSource())), interval))
            .as("over a wrapper of a DataSource that is no HikariDataSource").isNull();
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
