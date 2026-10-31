package io.github.dsudomoin.outbox.spring;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Apps for the auto-configuration's ITs: Boot's own DataSource over the tests' Postgres, fast relay settings. */
final class TestApps {

    private TestApps() {}

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class Boot {}

    /** The route of topic {@code orders}, recording what it is handed. */
    static final class OrdersRoute implements OutboxTopicHandler {

        private final RecordingHandler records = new RecordingHandler();

        @Override
        public Set<String> topics() {
            return Set.of("orders");
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            records.handle(record);
        }

        List<UUID> delivered() {
            return records.delivered().stream().map(OutboxRecord::messageId).toList();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithRoute {

        @Bean
        OrdersRoute ordersRoute() {
            return new OrdersRoute();
        }
    }

    /** Boot's datasource keys, no migrations, a fresh outbox and inbox table, the tests' fast relay settings. */
    static String[] properties(String... more) {
        return Stream.concat(Stream.of("spring.datasource.url=" + TestDataSources.jdbcUrl(),
            "spring.datasource.username=" + TestDataSources.user(),
            "spring.datasource.password=" + TestDataSources.password(),
            "spring.flyway.enabled=false", "spring.liquibase.enabled=false",
            "outbox.table=" + TestSql.uniqueTable(), "outbox.inbox.table=" + TestSql.uniqueTable(),
            "outbox.shards=4", "outbox.relay.workers=2", "outbox.relay.idle-poll-interval=20ms",
            "outbox.relay.min-tick-interval=5ms", "outbox.relay.batch-timeout=5s",
            "outbox.relay.shutdown-timeout=5s", "outbox.relay.retry.initial-delay=50ms",
            "outbox.relay.retry.max-delay=500ms"), Stream.of(more)).toArray(String[]::new);
    }

    static ApplicationContextRunner runner(String... properties) {
        return new ApplicationContextRunner().withUserConfiguration(Boot.class)
            .withPropertyValues(properties(properties));
    }

    /** Sends a message of {@code topic} in a transaction of the context's transaction manager; its id. */
    static UUID send(ApplicationContext context, String topic) {
        var outbox = context.getBean(Outbox.class);
        return new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
            .execute(status -> outbox.send(OutboxMessage.of(topic, "k", "v".getBytes(UTF_8))));
    }

    static String table(ApplicationContext context) {
        return context.getBean(OutboxConfig.class).table();
    }

    /** On a connection of its own. */
    static long queryLong(String sql) throws SQLException {
        try (var c = TestDataSources.newDataSource().getConnection()) {
            return TestSql.queryLong(c, sql);
        }
    }

    /** On a connection of its own. */
    static void execute(String sql) throws SQLException {
        try (var c = TestDataSources.newDataSource().getConnection()) {
            TestSql.execute(c, sql);
        }
    }
}
