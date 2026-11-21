package io.github.dsudomoin.outbox.kora;

import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.internal.HeartbeatPoolConfig;
import jakarta.annotation.Nullable;
import java.time.Duration;
import ru.tinkoff.kora.database.jdbc.JdbcDatabase;

/** The heartbeat's own pool of one connection, derived from the Hikari pool behind the outbox's transactions. */
final class HeartbeatPool {

    private HeartbeatPool() {}

    /**
     * A new pool from {@link HeartbeatPoolConfig#of}, for transactions that are {@link KoraOutboxTransactions} over a
     * {@link JdbcDatabase} backed by Hikari; null for any other, which leaves nothing to copy.
     */
    @Nullable
    static HikariDataSource derive(OutboxTransactions db, Duration maintenanceInterval) {
        if (db instanceof KoraOutboxTransactions kora && kora.factory() instanceof JdbcDatabase jdbc
                && jdbc.value() instanceof HikariDataSource app) {
            return new HikariDataSource(HeartbeatPoolConfig.of(app, maintenanceInterval));
        }
        return null;
    }
}
