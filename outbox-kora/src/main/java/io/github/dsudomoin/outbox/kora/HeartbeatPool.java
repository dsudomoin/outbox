package io.github.dsudomoin.outbox.kora;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.time.Duration;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

/** The heartbeat's own pool of one connection, derived from the Hikari pool behind the outbox's transactions. */
final class HeartbeatPool {

    private HeartbeatPool() {}

    /**
     * {@code app}'s config with one connection, always open, named {@code <app pool>-outbox-heartbeat}, whose
     * construction never probes the database. A borrow may wait {@code connectionTimeout} and then validate for
     * {@code validationTimeout}; the first is capped at a quarter of {@code maintenanceInterval} and the second at half
     * of the first, so together they stay under 0.375 intervals, and neither goes below Hikari's 250 ms floor.
     */
    static HikariConfig config(HikariConfig app, Duration maintenanceInterval) {
        var config = new HikariConfig();
        app.copyStateTo(config);
        config.setMaximumPoolSize(1);
        config.setMinimumIdle(1);
        config.setIdleTimeout(0); // a fixed-size pool never idles out; Hikari WARNs on another value than its default
        config.setInitializationFailTimeout(-1);
        config.setPoolName(app.getPoolName() + "-outbox-heartbeat");
        long connection = Math.max(500, Math.min(app.getConnectionTimeout(), maintenanceInterval.toMillis() / 4));
        config.setConnectionTimeout(connection);
        config.setValidationTimeout(Math.max(250, Math.min(app.getValidationTimeout(), connection / 2)));
        return config;
    }

    /**
     * A new pool from {@link #config}, for transactions that are {@link KoraOutboxTransactions} over a
     * {@link JdbcDatabase} backed by Hikari; null for any other, which leaves nothing to copy.
     */
    @Nullable
    static HikariDataSource derive(OutboxTransactions db, Duration maintenanceInterval) {
        if (db instanceof KoraOutboxTransactions kora && kora.factory() instanceof JdbcDatabase jdbc
                && jdbc.value() instanceof HikariDataSource app) {
            return new HikariDataSource(config(app, maintenanceInterval));
        }
        return null;
    }
}
