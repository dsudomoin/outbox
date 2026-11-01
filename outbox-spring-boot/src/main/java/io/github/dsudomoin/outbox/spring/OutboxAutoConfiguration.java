package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.InboxRuntime;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxDeserializer;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxSerializer;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.OutboxTransactions;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import io.github.dsudomoin.outbox.protobuf.ProtobufOutbox;
import jakarta.annotation.Nullable;
import java.sql.SQLException;
import java.time.Duration;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Fallback;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;

/**
 * The outbox over the application's {@code DataSource} (the {@code @Primary} one of several), joining its
 * Spring-managed transactions, with settings under {@code outbox.*}. Records go to the {@link OutboxTopicHandler}
 * beans, each for the topics it claims, and every other topic to the {@link OutboxHandler} bean; dead letters to the
 * {@link OutboxDeadLetterHandler} bean, if any. A handler bean behind a Spring AOP proxy is called through it, so that
 * its advice ({@code @Transactional}, aspects) runs per record; the start fails when that advice could never run. The
 * outbox runs with the context's lifecycle ({@link OutboxLifecycle}).
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
 * <p>
 * {@link TypedOutbox TypedOutbox&lt;T&gt;}, {@link OutboxSerializer OutboxSerializer&lt;T&gt;} and
 * {@link OutboxDeserializer OutboxDeserializer&lt;T&gt;} are served for any {@code T} an injection point declares, so
 * that {@link TypedOutboxTopicHandler TypedOutboxTopicHandler&lt;T&gt;} and senders need no hand-written codec, the
 * JSON ones over the app's mapper ({@code outbox.json.mapper}). A bean of the app's for a concrete {@code T} wins for
 * that {@code T}. A handler reaches {@code TypedOutbox} through an {@code ObjectProvider}, as it does {@code Outbox}.
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
                ObjectProvider<HeartbeatPools> heartbeatPools, Environment environment, ListableBeanFactory beans) {
            var binder = Binder.get(environment);
            boolean dedicated = binder.bindOrCreate("outbox.heartbeat", OutboxProperties.Heartbeat.class)
                .dedicatedConnection();
            var handlers = HandlerBeans.collect(beans, handler, routes);
            Supplier<OutboxRuntime.Builder> builders = () -> {
                var builder = OutboxRuntime.builder(config, transactions, handlers.fallback())
                    .telemetry(telemetry(telemetry));
                handlers.routes().forEach(builder::route);
                deadLetterHandler.ifAvailable(builder::deadLetterHandler);
                owner.ifAvailable(ds -> builder.ownerConnections(SpringOutboxTransactions.autocommit(ds)));
                return builder;
            };
            builders.get().build(); // checks the config and the routes now; touches no database
            // the heartbeat's pool is derived at each start, from the app pool as it is then, its meters bound
            Supplier<OutboxLifecycle.Built> runtimes = () -> {
                var builder = builders.get();
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
                    var built = new OutboxLifecycle.Built(builder.build(), closePool);
                    handlers.logRoutes();
                    return built;
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

        /** An app's bean for a concrete {@code T} matches first, an own generic one by the fallback. */
        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Fallback
        @Nullable
        @SuppressWarnings("unchecked")
        <T> TypedOutbox<T> typedOutbox(ObjectProvider<Outbox> outbox, AutowireCapableBeanFactory factory) {
            var injectionPoint = TypedBeans.injectionPoint(factory);
            var type = TypedBeans.typeArgument(injectionPoint, TypedOutbox.class);
            if (type == null) {
                return null;
            }
            OutboxSerializer<T> serializer = TypedBeans.serializer(factory, type, injectionPoint.isRequired());
            return serializer != null
                ? new TypedOutbox<>(outbox.getObject(), serializer, (Class<T>) type.toClass()) : null;
        }

        /** Only with protobuf present, though {@link ProtobufOutbox} loads without it. */
        @Configuration(proxyBeanMethods = false)
        @ConditionalOnClass(name = "com.google.protobuf.Message")
        static class ProtobufConfiguration {

            @Bean
            @ConditionalOnMissingBean(ProtobufOutbox.class)
            ProtobufOutbox protobufOutbox(Outbox outbox) {
                return new ProtobufOutbox(outbox);
            }
        }
    }

    /**
     * The JSON codecs by {@code T} of the injection point. With no mapper, no injection point (a scan by type) or one
     * of a collection, a factory method returns null, Spring's "no bean": a required injection fails naming the type,
     * an optional one gets nothing, a scan skips it. The mapper is chosen by bean lookup at creation, not by a bean
     * condition, so no ordering after Boot's Jackson configurations is needed.
     */
    @Configuration(proxyBeanMethods = false)
    static class JsonConfiguration {

        /** Binds the key itself: an app's own OutboxConfig leaves no OutboxProperties bean. */
        @Bean
        @Lazy(false)
        TypedBeans.Json outboxJson(Environment environment, ObjectProvider<JsonCodec> codecs) {
            var mapper = Binder.get(environment).bindOrCreate("outbox.json", OutboxProperties.Json.class).mapper();
            return TypedBeans.Json.select(mapper, codecs.stream().toList());
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Fallback
        @Nullable
        <T> OutboxSerializer<T> outboxJsonSerializer(AutowireCapableBeanFactory factory, TypedBeans.Json json) {
            var type = TypedBeans.typeArgument(TypedBeans.injectionPoint(factory), OutboxSerializer.class);
            return type != null && json.codec() != null ? json.codec().serializer(TypedBeans.jsonType(type)) : null;
        }

        @Bean
        @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
        @Fallback
        @Nullable
        <T> OutboxDeserializer<T> outboxJsonDeserializer(AutowireCapableBeanFactory factory, TypedBeans.Json json) {
            var type = TypedBeans.typeArgument(TypedBeans.injectionPoint(factory), OutboxDeserializer.class);
            return type != null && json.codec() != null ? json.codec().deserializer(TypedBeans.jsonType(type)) : null;
        }

        /** Only with Jackson 2 present: no class that loads without it names its types. */
        @Configuration(proxyBeanMethods = false)
        @ConditionalOnClass(name = "com.fasterxml.jackson.databind.ObjectMapper")
        static class Jackson2Configuration {

            @Bean
            JsonCodec outboxJackson2Codec(ObjectProvider<com.fasterxml.jackson.databind.ObjectMapper> mappers) {
                return new Jackson2Codec(mappers);
            }
        }

        /** Only with Jackson 3 present: no class that loads without it names its types. */
        @Configuration(proxyBeanMethods = false)
        @ConditionalOnClass(name = "tools.jackson.databind.json.JsonMapper")
        static class Jackson3Configuration {

            @Bean
            JsonCodec outboxJackson3Codec(ObjectProvider<tools.jackson.databind.json.JsonMapper> mappers) {
                return new Jackson3Codec(mappers);
            }
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
