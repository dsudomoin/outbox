package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class JdbcTransactionsIT {

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private OutboxRuntime runtime;

    @AfterEach
    void close() {
        if (runtime != null) {
            runtime.close();
        }
    }

    @Test
    void sendOutsideInTxNamesTheHint() {
        var config = new TestOutboxConfig().relayEnabled(false);
        runtime = OutboxRuntime.builder(config, transactions, new RecordingHandler()).build();
        runtime.start();

        assertThatThrownBy(() -> runtime.outbox().send(OutboxMessage.of("t", "p".getBytes(UTF_8))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("JdbcTransactions.inTx");
    }

    @Test
    void rollbackRunsNoAfterCommitAction() {
        var runs = new AtomicInteger();

        assertThatThrownBy(() -> transactions.inTx(c -> {
            transactions.afterCommit(runs::incrementAndGet);
            throw new IllegalStateException("rollback");
        })).isInstanceOf(IllegalStateException.class).hasMessage("rollback");

        assertThat(runs).hasValue(0);
    }

    @Test
    void throwingAfterCommitActionDoesNotReachCaller() {
        var table = "jt_" + Long.toHexString(System.nanoTime());
        var later = new AtomicInteger();

        var result = transactions.inTx(c -> {
            try (var st = c.createStatement()) {
                st.execute("create table " + table + " (id int)");
            }
            transactions.afterCommit(() -> {
                throw new IllegalStateException("after commit");
            });
            transactions.afterCommit(later::incrementAndGet);
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(later).as("the next action still runs").hasValue(1);
        int rows = transactions.inTx(c -> {
            try (var st = c.createStatement(); var rs = st.executeQuery("select count(*) from " + table)) {
                rs.next();
                return rs.getInt(1);
            }
        });
        assertThat(rows).as("the commit stands").isZero();
    }

    @Test
    void nestedInTxJoinsOuter() {
        var commits = new AtomicInteger();

        transactions.inTx(outer -> {
            transactions.afterCommit(commits::incrementAndGet);
            transactions.inTx(inner -> {
                assertThat(inner).isSameAs(outer);
                assertThat(transactions.currentConnection()).isSameAs(outer);
                return null;
            });
            assertThat(commits).as("the inner call does not commit").hasValue(0);
            return null;
        });

        assertThat(commits).hasValue(1);
    }

    @Test
    void afterCommitDeduplicates() {
        var runs = new AtomicInteger();
        Runnable action = runs::incrementAndGet;

        transactions.inTx(c -> {
            transactions.afterCommit(action);
            transactions.afterCommit(action);
            return null;
        });

        assertThat(runs).hasValue(1);
    }

    @Test
    void currentConnectionIsNullOutsideInTx() {
        Connection inside = transactions.inTx(c -> c);

        assertThat(inside).isNotNull();
        assertThat(transactions.currentConnection()).isNull();
    }

    @Test
    void aFailedRollbackNeverCommitsThePartialWork() throws Exception {
        var table = table();
        var faulty = new Faulty("rollback");
        var db = new JdbcTransactions(faulty.source());

        assertThatThrownBy(() -> db.inTx(c -> {
            insert(c, table);
            throw new IllegalStateException("the call failed");
        })).isInstanceOf(IllegalStateException.class).hasMessage("the call failed")
                .satisfies(e -> assertThat(e.getSuppressed()).extracting(Throwable::getMessage)
                        .containsExactly("injected rollback"));

        assertThat(rows(table)).as("the session stayed alive: autocommit on would have committed it").isZero();
        assertThat(faulty.calls).as("a doubtful connection goes no further").contains("abort")
                .doesNotContain("setAutoCommit(true)");
        assertThat(db.currentConnection()).isNull();
    }

    @Test
    void aFailedResetAfterTheCommitIsNoFailureOfTheTransaction() throws Exception {
        var table = table();
        var faulty = new Faulty("setAutoCommit(true)");
        var db = new JdbcTransactions(faulty.source());
        var after = new AtomicInteger();

        var result = db.inTx(c -> {
            db.afterCommit(after::incrementAndGet);
            insert(c, table);
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(rows(table)).isOne();
        assertThat(after).as("committed: the action runs").hasValue(1);
        assertThat(faulty.calls).as("not given back with unknown settings").contains("abort");
    }

    @Test
    void aFailedCloseAfterTheCommitIsNoFailureOfTheTransaction() throws Exception {
        var table = table();
        var db = new JdbcTransactions(new Faulty("close").source());
        var after = new AtomicInteger();

        var result = db.inTx(c -> {
            db.afterCommit(after::incrementAndGet);
            insert(c, table);
            return "done";
        });

        assertThat(result).isEqualTo("done");
        assertThat(rows(table)).isOne();
        assertThat(after).hasValue(1);
    }

    @Test
    void aFailedResetAfterARollbackLeavesTheCallsFailureOnTop() throws Exception {
        var table = table();
        var faulty = new Faulty("setAutoCommit(true)");
        var db = new JdbcTransactions(faulty.source());

        assertThatThrownBy(() -> db.inTx(c -> {
            insert(c, table);
            throw new IllegalStateException("the call failed");
        })).isInstanceOf(IllegalStateException.class).hasMessage("the call failed")
                .satisfies(e -> assertThat(e.getSuppressed()).extracting(Throwable::getMessage)
                        .containsExactly("injected setAutoCommit(true)"));

        assertThat(rows(table)).isZero();
        assertThat(faulty.calls).contains("abort");
    }

    @Test
    void aFailedCloseAfterARollbackLeavesTheCallsFailureOnTop() throws Exception {
        var table = table();
        var db = new JdbcTransactions(new Faulty("close").source());

        assertThatThrownBy(() -> db.inTx(c -> {
            insert(c, table);
            throw new IllegalStateException("the call failed");
        })).isInstanceOf(IllegalStateException.class).hasMessage("the call failed")
                .satisfies(e -> assertThat(e.getSuppressed()).extracting(Throwable::getMessage)
                        .containsExactly("injected close"));

        assertThat(rows(table)).isZero();
    }

    @Test
    void aFailedCommitRollsBackAndRunsNoAfterCommitAction() throws Exception {
        var table = table();
        var faulty = new Faulty("commit");
        var db = new JdbcTransactions(faulty.source());
        var after = new AtomicInteger();

        assertThatThrownBy(() -> db.inTx(c -> {
            db.afterCommit(after::incrementAndGet);
            insert(c, table);
            return "done";
        })).isInstanceOf(OutboxSqlException.class).hasMessageContaining("injected commit");

        assertThat(rows(table)).isZero();
        assertThat(after).hasValue(0);
        assertThat(faulty.calls).as("a clean rollback: the connection goes back as usual").contains("rollback",
                "setAutoCommit(true)").doesNotContain("abort");
    }

    @Test
    void anErrorFromTheRollbackStillEndsTheConnectionAndLeavesTheCallsFailureOnTop() throws Exception {
        var table = table();
        var faulty = new Faulty("rollback");
        faulty.thrown = new AssertionError("injected error");
        var db = new JdbcTransactions(faulty.source());

        assertThatThrownBy(() -> db.inTx(c -> {
            insert(c, table);
            throw new IllegalStateException("the call failed");
        })).isInstanceOf(IllegalStateException.class).hasMessage("the call failed")
                .satisfies(e -> assertThat(e.getSuppressed()).extracting(Throwable::getMessage)
                        .containsExactly("injected error"));

        assertThat(rows(table)).isZero();
        assertThat(faulty.calls).contains("abort", "close");
    }

    @Test
    void aDriverRethrowingTheCallsFailureFromTheRollbackStillEndsTheConnection() throws Exception {
        var table = table();
        var faulty = new Faulty("rollback");
        var failure = new SQLException("the driver's own");
        faulty.thrown = failure;
        var db = new JdbcTransactions(faulty.source());

        assertThatThrownBy(() -> db.inTx(c -> {
            insert(c, table);
            throw failure;
        })).isInstanceOf(OutboxSqlException.class).hasCause(failure);

        assertThat(rows(table)).isZero();
        assertThat(faulty.calls).contains("abort", "close");
    }

    private static String table() throws SQLException {
        var table = "jt_" + Long.toHexString(System.nanoTime());
        try (var c = TestDataSources.newDataSource().getConnection(); var st = c.createStatement()) {
            st.execute("create table " + table + " (id int)");
        }
        return table;
    }

    private static void insert(Connection c, String table) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute("insert into " + table + " values (1)");
        }
    }

    private static int rows(String table) throws SQLException {
        try (var c = TestDataSources.newDataSource().getConnection(); var st = c.createStatement();
                var rs = st.executeQuery("select count(*) from " + table)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    /**
     * Real connections on which {@code fault} ({@code rollback}, {@code commit}, {@code setAutoCommit(true)} or
     * {@code close}) fails once it is called, its session alive: the driver's call is not made, but for {@code close},
     * which is. Records the calls that end a transaction.
     */
    private static final class Faulty {

        final List<String> calls = new CopyOnWriteArrayList<>();
        /** What {@code fault} throws, instead of an {@link SQLException} of its own. */
        Throwable thrown;
        private final String fault;
        private final DataSource real = TestDataSources.newDataSource();

        Faulty(String fault) {
            this.fault = fault;
        }

        DataSource source() {
            return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                    new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
                        var result = invoke(method, real, args);
                        return result instanceof Connection c ? connection(c) : result;
                    });
        }

        private Connection connection(Connection c) {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                        var call = method.getName().equals("setAutoCommit") ? "setAutoCommit(" + args[0] + ")"
                                : method.getName();
                        calls.add(call);
                        if (call.equals(fault)) {
                            if (call.equals("close")) {
                                c.close();
                            }
                            throw thrown != null ? thrown : new SQLException("injected " + call);
                        }
                        return invoke(method, c, args);
                    });
        }

        private static Object invoke(Method method, Object on, Object[] args) throws Throwable {
            try {
                return method.invoke(on, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }
}
