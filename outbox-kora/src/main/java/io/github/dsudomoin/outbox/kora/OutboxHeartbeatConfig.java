package io.github.dsudomoin.outbox.kora;

import ru.tinkoff.kora.config.common.annotation.ConfigValueExtractor;

/** The heartbeat's config, read from HOCON path {@code outbox.heartbeat} in Kora apps only; every key is optional. */
@ConfigValueExtractor
public interface OutboxHeartbeatConfig {

    /**
     * Whether the heartbeat gets a pool of its own, so that business load that exhausts the app's pool does not starve
     * its beats: one connection, always open, copied from the Hikari pool of the {@code JdbcDatabase} behind the
     * outbox's transactions, with the same database, role, driver properties and meters, named after that pool with
     * {@code -outbox-heartbeat} appended, its connection and validation timeouts capped by
     * {@code outbox.partitions.maintenanceInterval}. Count its connection into {@code max_connections}, the role's
     * {@code CONNECTION LIMIT} and a pooler's per-user pool. An {@link OutboxHeartbeat}-tagged factory wins over this
     * key; {@code false} keeps the heartbeat on the app's connections.
     */
    default boolean dedicatedConnection() {
        return true;
    }
}
