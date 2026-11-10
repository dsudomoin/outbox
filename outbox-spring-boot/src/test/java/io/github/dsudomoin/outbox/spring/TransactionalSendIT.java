package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.queryLong;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.table;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

import io.github.dsudomoin.outbox.Inbox;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxSerializationException;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.internal.UuidV7;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/** Sends through {@code @Transactional} service beans: one transaction with the business writes. */
class TransactionalSendIT {

    /** A business payload whose serializer always fails. */
    record Order(UUID id) {}

    static class Orders {

        private final JdbcTemplate jdbc;
        private final Outbox outbox;
        private final TypedOutbox<Order> failing;
        private final String table;

        Orders(JdbcTemplate jdbc, Outbox outbox, String table) {
            this.jdbc = jdbc;
            this.outbox = outbox;
            this.failing = new TypedOutbox<>(outbox, (topic, order) -> {
                throw new IllegalArgumentException("cannot serialize " + order);
            }, Order.class);
            this.table = table;
        }

        /** The order's row and its event; the event's id. */
        @Transactional
        public UUID createOrder(UUID id) {
            jdbc.update("insert into " + table + " (id) values (?)", id);
            return outbox.send(OutboxMessage.of("orders", id.toString(), "created".getBytes(UTF_8)));
        }

        @Transactional
        public void createOrderThenFail(UUID id) {
            createOrder(id);
            throw new RuntimeException("rollback");
        }

        @Transactional
        public void createOrderWithFailingSend(UUID id) {
            jdbc.update("insert into " + table + " (id) values (?)", id);
            failing.send("orders", id.toString(), new Order(id));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Service {

        @Bean
        Orders orders(JdbcTemplate jdbc, Outbox outbox, @Value("${business.table}") String table) {
            return new Orders(jdbc, outbox, table);
        }
    }

    record OrderPlaced(UUID id) {}

    static class PublishingOrders {

        private final JdbcTemplate jdbc;
        private final ApplicationEventPublisher events;
        private final String table;

        PublishingOrders(JdbcTemplate jdbc, ApplicationEventPublisher events, String table) {
            this.jdbc = jdbc;
            this.events = events;
            this.table = table;
        }

        @Transactional
        public void place(UUID id, boolean fail) {
            jdbc.update("insert into " + table + " (id) values (?)", id);
            events.publishEvent(new OrderPlaced(id));
            if (fail) {
                throw new RuntimeException("rollback");
            }
        }
    }

    /** Sends and accepts in the after-phases, where the business transaction is over: what each call ended with. */
    static class AfterPhases {

        private final Outbox outbox;
        private final Inbox inbox;
        final Map<String, Object> outcomes = new ConcurrentHashMap<>();

        AfterPhases(Outbox outbox, Inbox inbox) {
            this.outbox = outbox;
            this.inbox = inbox;
        }

        @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
        public void afterCommit(OrderPlaced event) {
            attempt("commit", event);
        }

        @TransactionalEventListener(phase = TransactionPhase.AFTER_ROLLBACK)
        public void afterRollback(OrderPlaced event) {
            attempt("rollback", event);
        }

        private void attempt(String phase, OrderPlaced event) {
            outcomes.put(phase + ".send", outcome(() -> outbox.send(
                OutboxMessage.of("orders", event.id().toString(), phase.getBytes(UTF_8)))));
            outcomes.put(phase + ".tryAccept", outcome(() -> inbox.tryAccept(UuidV7.next())));
        }

        private static Object outcome(Supplier<?> call) {
            try {
                return call.get();
            } catch (RuntimeException e) {
                return e;
            }
        }
    }

    /** Boot's transaction manager without JPA, on the default pool (auto-commit on). */
    @Configuration(proxyBeanMethods = false)
    static class AfterPhasesApp {

        @Bean
        DataSourceTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }

        @Bean
        PublishingOrders publishingOrders(JdbcTemplate jdbc, ApplicationEventPublisher events,
                                          @Value("${business.table}") String table) {
            return new PublishingOrders(jdbc, events, table);
        }

        @Bean
        AfterPhases afterPhases(Outbox outbox, Inbox inbox) {
            return new AfterPhases(outbox, inbox);
        }
    }

    private static ApplicationContextRunner app(String business) throws Exception {
        TestApps.execute("create table " + business
            + " (id uuid primary key, xid xid8 not null default pg_current_xact_id())");
        return runner("business.table=" + business).withUserConfiguration(WithRoute.class, Service.class);
    }

    @Test
    void rollbackLeavesNeitherOrderNorEvent() throws Exception {
        var business = TestSql.uniqueTable();
        app(business).run(context -> {
            var id = UUID.randomUUID();

            assertThatThrownBy(() -> context.getBean(Orders.class).createOrderThenFail(id))
                .isInstanceOf(RuntimeException.class).hasMessage("rollback");

            assertThat(queryLong("select count(*) from " + business)).isZero();
            assertThat(queryLong("select count(*) from " + table(context))).isZero();
        });
    }

    @Test
    void failingSendRollsBackBusiness() throws Exception {
        var business = TestSql.uniqueTable();
        app(business).run(context -> {
            assertThatThrownBy(() -> context.getBean(Orders.class).createOrderWithFailingSend(UUID.randomUUID()))
                .isInstanceOf(OutboxSerializationException.class);

            assertThat(queryLong("select count(*) from " + business)).isZero();
            assertThat(queryLong("select count(*) from " + table(context))).isZero();
        });
    }

    @Test
    void sameTransactionId() throws Exception {
        var business = TestSql.uniqueTable();
        app(business).run(context -> {
            var id = UUID.randomUUID();

            var messageId = context.getBean(Orders.class).createOrder(id);

            var jdbc = context.getBean(JdbcTemplate.class);
            assertThat(jdbc.queryForObject("select xid::text from " + table(context) + " where message_id = ?",
                String.class, messageId))
                .isEqualTo(jdbc.queryForObject("select xid::text from " + business + " where id = ?", String.class,
                    id));
        });
    }

    @Test
    void afterCommitAndAfterRollbackListenersRefused() throws Exception {
        var business = TestSql.uniqueTable();
        app(business).withUserConfiguration(AfterPhasesApp.class).run(context -> {
            var orders = context.getBean(PublishingOrders.class);
            var committed = UUID.randomUUID();

            orders.place(committed, false);
            assertThatThrownBy(() -> orders.place(UUID.randomUUID(), true)).hasMessage("rollback");

            var outcomes = context.getBean(AfterPhases.class).outcomes;
            assertThat(outcomes).containsOnlyKeys("commit.send", "commit.tryAccept", "rollback.send",
                "rollback.tryAccept");
            for (var phase : new String[] {"commit", "rollback"}) {
                assertThat(outcomes.get(phase + ".send")).as(phase + ".send")
                    .asInstanceOf(throwable(IllegalStateException.class))
                    .hasMessageContaining("Outbox.send must be called inside @Transactional or TransactionTemplate");
                assertThat(outcomes.get(phase + ".tryAccept")).as(phase + ".tryAccept")
                    .asInstanceOf(throwable(IllegalStateException.class))
                    .hasMessageContaining("Inbox.tryAccept and tryAcceptAll must be called inside");
            }
            assertThat(queryLong("select count(*) from " + business)).isOne();
            assertThat(queryLong("select count(*) from " + table(context))).as("outbox rows").isZero();
            assertThat(queryLong("select count(*) from " + context.getBean(OutboxConfig.class).inbox().table()))
                .as("inbox markers").isZero();
        });
    }
}
