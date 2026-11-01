package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/** A connection whose rollback failed never goes back into a real pool, and the pool serves the next call. */
class JdbcTransactionsOnAPoolIT {

    @Test
    void aFailedRollbackCommitsNothingAndThePoolReplacesTheConnection() throws Exception {
        var table = "jt_" + Long.toHexString(System.nanoTime());
        try (var c = TestDb.connect()) {
            TestSql.execute(c, "create table " + table + " (id int)");
        }
        var config = new HikariConfig();
        config.setJdbcUrl(TestDb.jdbcUrl());
        config.setUsername(TestDb.user());
        config.setPassword(TestDb.password());
        config.setMaximumPoolSize(1);
        try (var pool = new HikariDataSource(config)) {
            var failing = new AtomicBoolean(true);
            var db = new JdbcTransactions(failingRollback(pool, failing));
            int[] pid = new int[1];

            assertThatThrownBy(() -> db.inTx(c -> {
                pid[0] = pid(c);
                TestSql.execute(c, "insert into " + table + " values (1)");
                throw new IllegalStateException("the call failed");
            })).isInstanceOf(IllegalStateException.class);

            failing.set(false);
            int next = db.inTx(JdbcTransactionsOnAPoolIT::pid);
            assertThat(next).as("another session, on a pool of one").isNotEqualTo(pid[0]);
            try (var c = TestDb.connect()) {
                assertThat(TestSql.queryLong(c, "select count(*) from " + table)).isZero();
            }
        }
    }

    private static int pid(Connection c) throws SQLException {
        return (int) TestSql.queryLong(c, "select pg_backend_pid()");
    }

    private static DataSource failingRollback(DataSource pool, AtomicBoolean failing) {
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    var result = invoke(method, pool, args);
                    return result instanceof Connection c ? failingRollback(c, failing) : result;
                });
    }

    private static Connection failingRollback(Connection c, AtomicBoolean failing) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("rollback") && failing.get()) {
                        throw new SQLException("injected rollback");
                    }
                    return invoke(method, c, args);
                });
    }

    private static Object invoke(java.lang.reflect.Method method, Object on, Object[] args) throws Throwable {
        try {
            return method.invoke(on, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
