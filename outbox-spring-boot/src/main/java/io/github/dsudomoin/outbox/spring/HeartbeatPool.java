package io.github.dsudomoin.outbox.spring;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.time.Duration;

/**
 * The heartbeat's own pool of one connection, derived from the Hikari pool behind the outbox's transactions. Loaded
 * only where Hikari is on the classpath ({@code OutboxAutoConfiguration.HeartbeatPoolConfiguration}).
 */
final class HeartbeatPool {

    private HeartbeatPool() {}

    /**
     * {@code app}'s config with one connection, always open, named {@code <app pool>-outbox-heartbeat}
     * ({@code outbox-heartbeat} while the app pool has no name: Boot names it only with {@code spring.datasource.name},
     * Hikari when it starts), whose construction never probes the database. A borrow may wait
     * {@code connectionTimeout} and then validate for {@code validationTimeout}; the first is capped at a quarter of
     * {@code maintenanceInterval} and the second at half of the first, so from a 2 s interval up together they stay
     * within 0.375 intervals. Below it the floors hold: the first never goes under 500 ms, the second never under
     * Hikari's 250 ms, 750 ms in all.
     */
    static HikariConfig config(HikariConfig app, Duration maintenanceInterval) {
        var config = new HikariConfig();
        app.copyStateTo(config);
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(1);
        config.setIdleTimeout(0); // a fixed-size pool never idles out; Hikari WARNs on another value than its default
        config.setInitializationFailTimeout(-1);
        config.setPoolName(app.getPoolName() != null ? app.getPoolName() + "-outbox-heartbeat" : "outbox-heartbeat");
        long connection = Math.max(500, Math.min(app.getConnectionTimeout(), maintenanceInterval.toMillis() / 4));
        config.setConnectionTimeout(connection);
        config.setValidationTimeout(Math.max(250, Math.min(app.getValidationTimeout(), connection / 2)));
        return config;
    }

    /**
     * A new pool from {@link #config}, for transactions that are {@link SpringOutboxTransactions} over a DataSource
     * that is, or unwraps to, a {@link HikariDataSource}; null for any other, which leaves nothing to copy.
     */
    @Nullable
    static HikariDataSource derive(OutboxTransactions db, Duration maintenanceInterval) throws SQLException {
        if (db instanceof SpringOutboxTransactions spring && spring.dataSource().isWrapperFor(HikariDataSource.class)) {
            var app = spring.dataSource().unwrap(HikariDataSource.class);
            return new HikariDataSource(config(app, maintenanceInterval));
        }
        return null;
    }
}
