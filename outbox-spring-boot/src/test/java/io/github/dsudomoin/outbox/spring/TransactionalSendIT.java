package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.TestApps.queryLong;
import static io.github.dsudomoin.outbox.spring.TestApps.runner;
import static io.github.dsudomoin.outbox.spring.TestApps.table;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxSerializationException;
import io.github.dsudomoin.outbox.TypedOutbox;
import io.github.dsudomoin.outbox.spring.TestApps.WithRoute;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

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
}
