package io.github.dsudomoin.outbox.internal;

import com.zaxxer.hikari.HikariConfig;
import java.time.Duration;

/**
 * The config of the heartbeat's own pool, derived from the Hikari pool behind the outbox's transactions; for the Kora
 * and Spring adapters, loaded only where Hikari is on the classpath.
 */
public final class HeartbeatPoolConfig {

    private HeartbeatPoolConfig() {}

    /**
     * {@code app}'s config with one connection, always open, named {@code <app pool>-outbox-heartbeat}
     * ({@code outbox-heartbeat} while the app pool has no name: Boot names it only with {@code spring.datasource.name},
     * Hikari when it starts), whose construction never probes the database. A borrow may wait
     * {@code connectionTimeout} and then validate for {@code validationTimeout}; the first is capped at a quarter of
     * {@code maintenanceInterval} and the second at half of the first, so from a 2 s interval up together they stay
     * within 0.375 intervals. Below it the floors hold: the first never goes under 500 ms, the second never under
     * Hikari's 250 ms, 750 ms in all.
     */
    public static HikariConfig of(HikariConfig app, Duration maintenanceInterval) {
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
}
