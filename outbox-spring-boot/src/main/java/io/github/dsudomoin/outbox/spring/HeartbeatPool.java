package io.github.dsudomoin.outbox.spring;

import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.internal.HeartbeatPoolConfig;
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
     * A new pool from {@link HeartbeatPoolConfig#of}, for transactions that are {@link SpringOutboxTransactions} over
     * a DataSource that is, or unwraps to, a {@link HikariDataSource}; null for any other, which leaves nothing to copy.
     */
    @Nullable
    static HikariDataSource derive(OutboxTransactions db, Duration maintenanceInterval) throws SQLException {
        if (db instanceof SpringOutboxTransactions spring && spring.dataSource().isWrapperFor(HikariDataSource.class)) {
            var app = spring.dataSource().unwrap(HikariDataSource.class);
            return new HikariDataSource(HeartbeatPoolConfig.of(app, maintenanceInterval));
        }
        return null;
    }
}
