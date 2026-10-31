package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.InboxRuntime;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.time.Duration;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

/**
 * The outbox over the application's {@code DataSource} (the {@code @Primary} one of several), joining its
 * Spring-managed transactions, with settings under {@code outbox.*}. Records go to the {@link OutboxTopicHandler}
 * beans, each for the topics it claims, and every other topic to the {@link OutboxHandler} bean; dead letters to the
 * {@link OutboxDeadLetterHandler} bean, if any. The outbox runs with the context's lifecycle ({@link OutboxLifecycle}).
 * With {@code outbox.enabled=false} there is no outbox, the inbox only. {@link Inbox} is built, and its table created,
 * only when a bean injects it. With an {@link OutboxOwner} DataSource the DDL runs on that one. The heartbeat, which
 * keeps this instance's row fresh and reads the reports that guard routed sends, gets a pool of its own, so business
 * load that exhausts the app's pool does not starve it: one connection, copied from the {@code HikariDataSource} the
 * outbox's DataSource is or unwraps to, and closed after the runtime ({@code outbox.heartbeat.dedicated-connection}).
 * An {@link OutboxHeartbeat} DataSource replaces it, and {@code outbox.heartbeat.dedicated-connection=false} keeps the
 * heartbeat on the app's connections.
 * <p>
 * An app's own {@link OutboxConfig} bean replaces the {@code outbox.*} settings of the core, an own
 * {@link OutboxTransactions} bean {@link SpringOutboxTransactions}.
 */
@AutoConfiguration(afterName = {
    "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration", // Boot 3
    "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration" // Boot 4
})
@ConditionalOnClass(DataSource.class)
public final class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(OutboxTransactions.class)
    SpringOutboxTransactions outboxTransactions(DataSource dataSource) {
        return new SpringOutboxTransactions(dataSource);
    }

    private static OutboxTelemetry telemetry(ObjectProvider<OutboxTelemetry> telemetry) {
        return telemetry.getIfAvailable(() -> OutboxTelemetry.NONE);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean(OutboxConfig.class)
    @EnableConfigurationProperties(OutboxProperties.class)
    static class PropertiesConfiguration {}

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "outbox", name = "enabled", matchIfMissing = true)
    static class RuntimeConfiguration {

        /** Eager under global lazy init: nothing else would start the relay; stopped on a failed refresh too. */
        @Bean(destroyMethod = "stop")
        @Lazy(false)
        @DependsOnDatabaseInitialization
        OutboxLifecycle outboxLifecycle(OutboxConfig config, OutboxTransactions transactions,
                ObjectProvider<OutboxHandler> handler, ObjectProvider<OutboxTopicHandler> routes,
                ObjectProvider<OutboxDeadLetterHandler> deadLetterHandler, ObjectProvider<OutboxTelemetry> telemetry,
                @OutboxOwner ObjectProvider<DataSource> owner, @OutboxHeartbeat ObjectProvider<DataSource> heartbeat,
                ObjectProvider<HeartbeatPools> heartbeatPools, Environment environment) {
            var binder = Binder.get(environment);
            boolean dedicated = binder.bindOrCreate("outbox.heartbeat", OutboxProperties.Heartbeat.class)
                .dedicatedConnection();
            Supplier<OutboxLifecycle.Built> runtimes = () -> {
                var builder = OutboxRuntime.builder(config, transactions, handler.getIfUnique())
                    .telemetry(telemetry(telemetry));
                routes.orderedStream().forEach(builder::route);
                deadLetterHandler.ifAvailable(builder::deadLetterHandler);
                owner.ifAvailable(ds -> builder.ownerConnections(SpringOutboxTransactions.autocommit(ds)));
                HeartbeatPools.Pool pool = null;
                var own = heartbeat.getIfAvailable();
                if (own != null) {
                    builder.heartbeatConnections(SpringOutboxTransactions.autocommit(own));
                } else if (dedicated) {
                    pool = derivePool(config, transactions, heartbeatPools.getIfAvailable());
                    if (pool != null) {
                        builder.heartbeatConnections(SpringOutboxTransactions.autocommit(pool.connections()));
                    }
                }
                Runnable closePool = pool != null ? pool.close() : () -> {};
                try {
                    return new OutboxLifecycle.Built(builder.build(), closePool);
                } catch (RuntimeException | Error e) {
                    closePool.run();
                    throw e;
                }
            };
            int phase = binder.bindOrCreate("outbox.lifecycle", OutboxProperties.Lifecycle.class).phase();
            return new OutboxLifecycle(runtimes, phase);
        }

        /** A pool of the heartbeat's own derived from the outbox's Hikari pool; null without one or when it fails. */
        @Nullable
        private static HeartbeatPools.Pool derivePool(OutboxConfig config, OutboxTransactions transactions,
                @Nullable HeartbeatPools pools) {
            var log = LoggerFactory.getLogger(OutboxAutoConfiguration.class);
            try {
                var pool = pools != null ? pools.derive(transactions, config.partitions().maintenanceInterval()) : null;
                if (pool == null) {
                    log.info("outbox heartbeat gets no pool of its own: the outbox's transactions are not"
                        + " SpringOutboxTransactions over a HikariDataSource, whose pool it would copy;"
                        + " give it one as a DataSource bean qualified @OutboxHeartbeat");
                }
                return pool;
            } catch (SQLException | RuntimeException e) { // a broken pool is no worse than none
                log.warn("outbox heartbeat of {} could not derive a pool of its own from the app's and shares the"
                    + " application's connections: {}", config.table(), e.toString());
                return null;
            }
        }

        @Bean
        Outbox outbox(OutboxLifecycle lifecycle) {
            return new LifecycleOutbox(lifecycle);
        }

        @Bean
        OutboxAdmin outboxAdmin(OutboxLifecycle lifecycle) {
            return new LifecycleOutboxAdmin(lifecycle);
        }

        /** The relay of the runtime current when first injected, after the start: for tests and later code. */
        @Bean(destroyMethod = "") // the lifecycle closes it
        @Lazy
        OutboxRelay outboxRelay(OutboxLifecycle lifecycle) {
            return lifecycle.runtime().relay();
        }
    }

    /** Derives the heartbeat's own pool ({@link HeartbeatPool#derive}); null when there is no Hikari pool to copy. */
    @FunctionalInterface
    interface HeartbeatPools {

        @Nullable
        Pool derive(OutboxTransactions transactions, Duration maintenanceInterval) throws SQLException;

        /** The derived pool's connections, and its close. */
        record Pool(DataSource connections, Runnable close) {}
    }

    /** Only with Hikari present: no class that loads without it names its types. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "com.zaxxer.hikari.HikariDataSource")
    static class HeartbeatPoolConfiguration {

        @Bean
        HeartbeatPools outboxHeartbeatPools() {
            return (transactions, interval) -> {
                var pool = HeartbeatPool.derive(transactions, interval);
                return pool != null ? new HeartbeatPools.Pool(pool, pool::close) : null;
            };
        }
    }

    /** Lazy, as a SmartLifecycle bean would be created whether used or not: its users start later anyway. */
    @Configuration(proxyBeanMethods = false)
    static class InboxConfiguration {

        @Bean(initMethod = "start", destroyMethod = "close")
        @Lazy
        @DependsOnDatabaseInitialization
        InboxRuntime inboxRuntime(OutboxConfig config, OutboxTransactions transactions,
                ObjectProvider<OutboxTelemetry> telemetry, @OutboxOwner ObjectProvider<DataSource> owner) {
            var builder = InboxRuntime.builder(config, transactions).telemetry(telemetry(telemetry));
            owner.ifAvailable(ds -> builder.ownerConnections(SpringOutboxTransactions.autocommit(ds)));
            return builder.build();
        }

        @Bean(destroyMethod = "") // the runtime closes it
        @Lazy
        @DependsOnDatabaseInitialization
        Inbox inbox(InboxRuntime inboxRuntime) {
            return inboxRuntime.inbox();
        }
    }
}
