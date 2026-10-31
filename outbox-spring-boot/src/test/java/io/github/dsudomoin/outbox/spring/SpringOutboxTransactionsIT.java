package io.github.dsudomoin.outbox.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Sends inside Spring-managed transactions: one physical PostgreSQL transaction with the business writes. */
class SpringOutboxTransactionsIT {

    private static final String HINT = "@Transactional or TransactionTemplate";

    /**
     * Boot's DataSource (Hikari), transaction manager, JdbcTemplate and JPA; the runtime built and started by hand, the
     * outbox's own auto-configuration excluded.
     */
    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    @AutoConfigurationPackage // the JPA entity below
    static class App {

        @Bean
        TestOutboxConfig outboxConfig() {
            return new TestOutboxConfig();
        }

        @Bean
        RecordingHandler handler() {
            return new RecordingHandler();
        }

        @Bean
        SpringOutboxTransactions outboxTransactions(DataSource dataSource) {
            return new SpringOutboxTransactions(dataSource);
        }

        @Bean(destroyMethod = "close")
        OutboxRuntime outboxRuntime(TestOutboxConfig config, SpringOutboxTransactions transactions,
                                    RecordingHandler handler) {
            var runtime = OutboxRuntime.builder(config, transactions, handler).build();
            runtime.start();
            return runtime;
        }
    }

    /** The app's DataSource is a proxy that hands out the connection of the caller's transaction. */
    @Configuration(proxyBeanMethods = false)
    static class TransactionAwareDataSource {

        @Bean
        DataSource dataSource() {
            return new TransactionAwareDataSourceProxy(TestDataSources.newDataSource());
        }
    }

    @Entity
    @Table(name = "spring_jpa_order")
    static class JpaOrder {
        @Id
        UUID id;

        JpaOrder() {}

        JpaOrder(UUID id) {
            this.id = id;
        }
    }

    private static ApplicationContextRunner runner(String... properties) {
        return new ApplicationContextRunner().withUserConfiguration(App.class)
            .withPropertyValues("spring.datasource.url=" + TestDataSources.jdbcUrl(),
                "spring.datasource.username=" + TestDataSources.user(),
                "spring.datasource.password=" + TestDataSources.password(),
                "spring.flyway.enabled=false", "spring.liquibase.enabled=false",
                "spring.autoconfigure.exclude=io.github.dsudomoin.outbox.spring.OutboxAutoConfiguration")
            .withPropertyValues(properties);
    }

    /** One context: the business table, the runtime, Boot's beans. */
    private record Ctx(AssertableApplicationContext context, String business) {

        static Ctx of(AssertableApplicationContext context) throws SQLException {
            assertThat(context).hasNotFailed();
            var business = TestSql.uniqueTable();
            ddl("create table " + business + " (id uuid primary key, xid xid8 not null default pg_current_xact_id())");
            return new Ctx(context, business);
        }

        /** On a connection of its own: the pool's may come without autocommit. */
        static void ddl(String sql) throws SQLException {
            try (var c = TestDataSources.newDataSource().getConnection()) {
                TestSql.execute(c, sql);
            }
        }

        DataSource dataSource() {
            return context.getBean(DataSource.class);
        }

        JdbcTemplate jdbc() {
            return context.getBean(JdbcTemplate.class);
        }

        OutboxRuntime runtime() {
            return context.getBean(OutboxRuntime.class);
        }

        RecordingHandler handler() {
            return context.getBean(RecordingHandler.class);
        }

        String outboxTable() {
            return context.getBean(TestOutboxConfig.class).table();
        }

        TransactionTemplate jdbcTx() {
            return new TransactionTemplate(new DataSourceTransactionManager(dataSource()));
        }

        TransactionTemplate jdbcTx(int propagation) {
            var tx = jdbcTx();
            tx.setPropagationBehavior(propagation);
            return tx;
        }

        /** Inserts a business row and sends a message, in the caller's transaction; the message id. */
        UUID insertAndSend(UUID businessId) {
            jdbc().update("insert into " + business + " (id) values (?)", businessId);
            return send();
        }

        UUID send() {
            return runtime().outbox().send(OutboxMessage.of("orders", "k", "v".getBytes(UTF_8)));
        }

        String businessXid(UUID id) {
            return jdbc().queryForObject("select xid::text from " + business + " where id = ?", String.class, id);
        }

        String outboxXid(UUID messageId) {
            return jdbc().queryForObject("select xid::text from " + outboxTable() + " where message_id = ?",
                String.class, messageId);
        }

        long businessRows(UUID id) {
            return jdbc().queryForObject("select count(*) from " + business + " where id = ?", Long.class, id);
        }

        long outboxRows(UUID messageId) {
            return jdbc().queryForObject("select count(*) from " + outboxTable() + " where message_id = ?",
                Long.class, messageId);
        }

        long outboxRows() {
            return jdbc().queryForObject("select count(*) from " + outboxTable(), Long.class);
        }
    }

    @Test
    void sendJoinsTheTransaction() {
        runner().run(context -> {
            var ctx = Ctx.of(context);
            var businessId = UUID.randomUUID();

            var messageId = ctx.jdbcTx().execute(status -> ctx.insertAndSend(businessId));

            assertThat(ctx.outboxXid(messageId)).isEqualTo(ctx.businessXid(businessId));
        });
    }

    @Test
    void rollbackLeavesNeither() {
        runner().run(context -> {
            var ctx = Ctx.of(context);
            var businessId = UUID.randomUUID();
            var messageId = new UUID[1];

            assertThatThrownBy(() -> ctx.jdbcTx().execute(status -> {
                messageId[0] = ctx.insertAndSend(businessId);
                throw new IllegalArgumentException("business failure");
            })).isInstanceOf(IllegalArgumentException.class);

            assertThat(messageId[0]).isNotNull();
            assertThat(ctx.businessRows(businessId)).isZero();
            assertThat(ctx.outboxRows(messageId[0])).isZero();
        });
    }

    @Test
    void outsideTransactionRefused() {
        runner().run(context -> {
            var ctx = Ctx.of(context);

            assertThatThrownBy(ctx::send).isInstanceOf(IllegalStateException.class).hasMessageContaining(HINT);

            assertThat(ctx.outboxRows()).isZero();
        });
    }

    @Test
    void otherDataSourceTransactionRefused() {
        runner().run(context -> {
            var ctx = Ctx.of(context);
            var other = new TransactionTemplate(new DataSourceTransactionManager(TestDataSources.newDataSource()));

            assertThatThrownBy(() -> other.execute(status -> ctx.send()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining(HINT);

            assertThat(ctx.outboxRows()).isZero();
        });
    }

    @Test
    void autocommitOffPool() {
        runner("spring.datasource.hikari.auto-commit=false").run(context -> {
            var ctx = Ctx.of(context);
            var other = new TransactionTemplate(new DataSourceTransactionManager(TestDataSources.newDataSource()));

            assertThatThrownBy(() -> other.execute(status -> ctx.send()))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining(HINT);
            var committed = ctx.jdbcTx().execute(status -> ctx.insertAndSend(UUID.randomUUID()));

            await().atMost(Duration.ofSeconds(10)).until(() -> !ctx.handler().delivered().isEmpty());
            assertThat(ctx.handler().delivered()).extracting(OutboxRecord::messageId).containsExactly(committed);
            assertThat(ctx.outboxRows()).isOne();
        });
    }

    @Test
    void jpaTransactionJoins() {
        runner().run(context -> {
            var ctx = Ctx.of(context);
            Ctx.ddl("create table if not exists spring_jpa_order"
                + " (id uuid primary key, xid xid8 not null default pg_current_xact_id())");
            var manager = context.getBean(PlatformTransactionManager.class);
            assertThat(manager).isInstanceOf(JpaTransactionManager.class);
            var tx = new TransactionTemplate(manager);
            var em = SharedEntityManagerCreator.createSharedEntityManager(context.getBean(EntityManagerFactory.class));

            var committedId = UUID.randomUUID();
            var messageId = tx.execute(status -> {
                em.persist(new JpaOrder(committedId));
                em.flush();
                return ctx.send();
            });
            assertThat(ctx.outboxXid(messageId)).isEqualTo(ctx.jdbc().queryForObject(
                "select xid::text from spring_jpa_order where id = ?", String.class, committedId));

            var rolledBackId = UUID.randomUUID();
            var rolledBack = new UUID[1];
            assertThatThrownBy(() -> tx.execute(status -> {
                em.persist(new JpaOrder(rolledBackId));
                em.flush();
                rolledBack[0] = ctx.send();
                throw new IllegalArgumentException("business failure");
            })).isInstanceOf(IllegalArgumentException.class);
            assertThat(ctx.jdbc().queryForObject("select count(*) from spring_jpa_order where id = ?", Long.class,
                rolledBackId)).isZero();
            assertThat(ctx.outboxRows(rolledBack[0])).isZero();
        });
    }

    @Test
    void requiresNewAndNested() {
        runner().run(context -> {
            var ctx = Ctx.of(context);
            var outer = ctx.jdbcTx();
            var inner = new UUID[2];

            assertThatThrownBy(() -> outer.execute(status -> {
                inner[0] = ctx.jdbcTx(TransactionDefinition.PROPAGATION_REQUIRES_NEW).execute(s -> ctx.send());
                ctx.send();
                throw new IllegalArgumentException("outer failure");
            })).isInstanceOf(IllegalArgumentException.class);
            assertThat(ctx.outboxRows(inner[0])).as("REQUIRES_NEW survives the outer rollback").isOne();
            assertThat(ctx.outboxRows()).isOne();

            var kept = outer.execute(status -> {
                var outerSend = ctx.send();
                inner[1] = ctx.jdbcTx(TransactionDefinition.PROPAGATION_NESTED).execute(s -> {
                    var id = ctx.send();
                    s.setRollbackOnly(); // back to the savepoint
                    return id;
                });
                return outerSend;
            });
            assertThat(ctx.outboxRows(kept)).isOne();
            assertThat(ctx.outboxRows(inner[1])).as("NESTED rolled back to its savepoint").isZero();
        });
    }

    @Test
    void afterCommitOnceAndSwallowed() {
        runner().run(context -> {
            var ctx = Ctx.of(context);
            var transactions = context.getBean(SpringOutboxTransactions.class);
            var businessId = UUID.randomUUID();
            var runs = new AtomicInteger();
            var sawCommit = new AtomicBoolean();
            Runnable action = () -> {
                runs.incrementAndGet();
                // another connection: the transaction's own is still bound here
                try (var c = TestDataSources.newDataSource().getConnection()) {
                    sawCommit.set(TestSql.queryLong(c, "select count(*) from " + ctx.business()) == 1);
                } catch (SQLException e) {
                    throw new AssertionError(e);
                }
                throw new IllegalStateException("after-commit boom");
            };

            try (var err = new CapturedStderr()) {
                var result = ctx.jdbcTx().execute(status -> {
                    ctx.jdbc().update("insert into " + ctx.business() + " (id) values (?)", businessId);
                    transactions.afterCommit(action);
                    transactions.afterCommit(action);
                    assertThat(runs).as("not before the commit").hasValue(0);
                    return "returned";
                });

                assertThat(result).isEqualTo("returned");
                assertThat(runs).hasValue(1);
                assertThat(sawCommit).isTrue();
                assertThat(err.text()).contains("WARN")
                    .contains("outbox after-commit action failed; the transaction is committed")
                    .contains("after-commit boom");
            }
        });
    }

    @Test
    void transactionConnectionUntouched() {
        runner("spring.datasource.hikari.auto-commit=false").run(context -> {
            var ctx = Ctx.of(context);

            boolean autoCommit = ctx.jdbcTx().execute(status -> {
                ctx.send();
                try {
                    return DataSourceUtils.getConnection(ctx.dataSource()).getAutoCommit();
                } catch (SQLException e) {
                    throw new AssertionError(e);
                }
            });
            assertThat(autoCommit).isFalse();

            try (Connection c = context.getBean(SpringOutboxTransactions.class).newConnection()) {
                assertThat(c.getAutoCommit()).isTrue();
            }
            try (Connection c = SpringOutboxTransactions.autocommit(ctx.dataSource()).newConnection()) {
                assertThat(c.getAutoCommit()).isTrue();
            }
        });
    }

    @Test
    void transactionAwareProxyUnwrapped() {
        runner().withUserConfiguration(TransactionAwareDataSource.class).run(context -> {
            var ctx = Ctx.of(context);
            assertThat(ctx.dataSource()).isInstanceOf(TransactionAwareDataSourceProxy.class);
            var businessId = UUID.randomUUID();

            var messageId = ctx.jdbcTx().execute(status -> ctx.insertAndSend(businessId));
            assertThat(ctx.outboxXid(messageId)).isEqualTo(ctx.businessXid(businessId));
            // nothing used the proxy in this transaction yet, so Spring bound no holder for it
            assertThat(ctx.outboxRows(ctx.jdbcTx().execute(status -> ctx.send()))).isOne();

            var rolledBack = UUID.randomUUID();
            List<ConnectionSource> sources = List.of(context.getBean(SpringOutboxTransactions.class),
                SpringOutboxTransactions.autocommit(ctx.dataSource()));
            assertThatThrownBy(() -> ctx.jdbcTx().execute(status -> {
                ctx.jdbc().update("insert into " + ctx.business() + " (id) values (?)", rolledBack);
                long transactionPid = ctx.jdbc().queryForObject("select pg_backend_pid()", Long.class);
                for (var source : sources) {
                    try (var c = source.newConnection()) {
                        assertThat(TestSql.queryLong(c, "select pg_backend_pid()")).as("a fresh connection")
                            .isNotEqualTo(transactionPid);
                    } catch (SQLException e) {
                        throw new AssertionError(e);
                    }
                }
                throw new IllegalArgumentException("business failure");
            })).isInstanceOf(IllegalArgumentException.class);
            assertThat(ctx.businessRows(rolledBack)).as("the transaction's work not committed by newConnection()")
                .isZero();
        });
    }

    @Test
    void translateGivesDataAccessException() {
        var transactions = new SpringOutboxTransactions(new DriverManagerDataSource());

        assertThat(transactions.translate(new SQLException("x", "40001")))
            .isInstanceOf(ConcurrencyFailureException.class);
        assertThat(transactions.translate(new SQLException("x", "ZZ999")))
            .isInstanceOf(UncategorizedSQLException.class);
        assertThat(transactions.transactionHint()).isEqualTo(HINT);
    }
}
