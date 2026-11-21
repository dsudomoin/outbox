package io.github.dsudomoin.outbox;

import static io.github.dsudomoin.outbox.testing.Intercepting.intercepting;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** A connection whose rollback failed never goes back into a real pool, and the pool serves the next call. */
class JdbcTransactionsOnAPoolIT {

    @Test
    void aFailedRollbackCommitsNothingAndThePoolReplacesTheConnection() throws Exception {
        var table = TestSql.uniqueTable();
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
            var db = new JdbcTransactions(intercepting(pool, (real, call, args) -> {
                if (call.getName().equals("rollback") && failing.get()) {
                    throw new SQLException("injected rollback");
                }
            }));
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
}
