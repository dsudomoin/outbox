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
import java.util.function.Supplier;
import javax.sql.DataSource;
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
 * only when a bean injects it. With an {@link OutboxOwner} DataSource the DDL runs on that one.
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
                @OutboxOwner ObjectProvider<DataSource> owner, Environment environment) {
            Supplier<OutboxRuntime> runtimes = () -> {
                var builder = OutboxRuntime.builder(config, transactions, handler.getIfUnique())
                    .telemetry(telemetry(telemetry));
                routes.orderedStream().forEach(builder::route);
                deadLetterHandler.ifAvailable(builder::deadLetterHandler);
                owner.ifAvailable(ds -> builder.ownerConnections(SpringOutboxTransactions.autocommit(ds)));
                return builder.build();
            };
            int phase = Binder.get(environment).bindOrCreate("outbox.lifecycle", OutboxProperties.Lifecycle.class)
                .phase();
            return new OutboxLifecycle(runtimes, phase);
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
