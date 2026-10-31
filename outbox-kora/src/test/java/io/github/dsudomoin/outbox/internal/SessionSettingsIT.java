package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A shared pool of one session whose {@code connectionInitSql} sets the timeouts: what the bootstrap, maintenance and
 * the admin set on it for their own work, they give back on every path, so that session still has the pool's values;
 * and a session that cannot get them back leaves the pool.
 */
class SessionSettingsIT {

    private static final String SETTINGS = "select current_setting('statement_timeout') || ' '"
            + " || current_setting('lock_timeout') || ' ' || current_setting('idle_session_timeout')";
    private static final String POOLS = "123s 7s 10min";

    private final HikariDataSource pool = pool();
    private final JdbcTransactions pooled = new JdbcTransactions(pool);
    private final TestOutboxConfig config = new TestOutboxConfig();
    private final int session = pid();

    @AfterEach
    void close() {
        pool.close();
    }

    @Test
    void theBootstrapGivesThemBack() throws Exception {
        assertThat(settings()).as("the pool's").isEqualTo(POOLS);

        new OutboxSchema(pooled, config).start();

        assertThat(pid()).as("one session").isEqualTo(session);
        assertThat(settings()).isEqualTo(POOLS);
    }

    @Test
    void aBootstrapThatFailsGivesThemBackToo() {
        var app = new JdbcTransactions(TestDataSources.newDataSource());
        var schema = new OutboxSchema(app, pooled, config.schemaBootstrap(false)); // no table: the check fails

        assertThatThrownBy(schema::start).hasMessageContaining("outbox schema check of " + config.table());

        assertThat(pid()).isEqualTo(session);
        assertThat(settings()).isEqualTo(POOLS);
    }

    @Test
    void maintenanceAndTheAdminGiveThemBack() throws Exception {
        var schema = new OutboxSchema(new JdbcTransactions(TestDataSources.newDataSource()), config);
        schema.start();
        var maintenance = new Maintenance(pooled, schema.names(), schema.config(), OutboxTelemetry.NONE);
        var admin = new PgOutboxAdmin(pooled, schema.names(), schema.config(), maintenance, () -> {});

        assertThat(maintenance.runOnce()).isTrue();
        assertThat(settings()).as("after a maintenance run").isEqualTo(POOLS);
        assertThatThrownBy(() -> admin.restoreOrphan(schema.names().partitionPrefix() + "1", 0))
                .isInstanceOf(IllegalArgumentException.class); // under the maintenance lock

        assertThat(pid()).isEqualTo(session);
        assertThat(settings()).as("after an admin call that failed").isEqualTo(POOLS);
    }

    @Test
    void theInboxGivesThemBack() throws Exception {
        var inbox = new PgInbox(pooled, config, OutboxTelemetry.NONE);
        inbox.start();
        assertThat(settings()).as("after its bootstrap").isEqualTo(POOLS);

        inbox.runMaintenance();

        assertThat(pid()).isEqualTo(session);
        assertThat(settings()).as("after its maintenance").isEqualTo(POOLS);
    }

    /** Giving them back fails on a live session: it is aborted, so the pool hands out a new one with its values. */
    @Test
    void aSessionThatCannotGetThemBackLeavesThePool() {
        var failing = new AtomicBoolean(true);
        var schema = new OutboxSchema(new JdbcTransactions(failingRestores(failing)), config);

        assertThatThrownBy(schema::start).hasMessageContaining("injected");
        failing.set(false);

        assertThat(pid()).as("a new session").isNotEqualTo(session);
        assertThat(settings()).isEqualTo(POOLS);
    }

    private static HikariDataSource pool() {
        var hikari = new HikariConfig();
        hikari.setJdbcUrl(TestDataSources.jdbcUrl());
        hikari.setUsername(TestDataSources.user());
        hikari.setPassword(TestDataSources.password());
        hikari.setMaximumPoolSize(1);
        hikari.setMinimumIdle(1);
        hikari.setConnectionTimeout(5_000); // a second borrow inside a call fails the test, rather than hang
        hikari.setConnectionInitSql("set statement_timeout = '123s'; set lock_timeout = '7s';"
                + " set idle_session_timeout = '10min'");
        return new HikariDataSource(hikari);
    }

    private int pid() {
        try (var c = pool.getConnection(); var st = c.createStatement();
                var rs = st.executeQuery("select pg_backend_pid()")) {
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private String settings() {
        try (var c = pool.getConnection(); var st = c.createStatement(); var rs = st.executeQuery(SETTINGS)) {
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The pool, where giving a setting back to its session's own value fails while {@code failing}, with an error that
     * leaves the session alive (no pool evicts on {@code XX000}).
     */
    private DataSource failingRestores(AtomicBoolean failing) {
        return proxy(DataSource.class, pool, (method, args) -> {
            if (!method.getName().equals("getConnection")) {
                return invoke(method, pool, args);
            }
            var c = (Connection) invoke(method, pool, args);
            return proxy(Connection.class, c, (m, a) -> {
                if (failing.get() && m.getName().equals("prepareStatement")
                        && a[0].equals("select set_config(?, ?, false)")) {
                    throw new SQLException("injected", "XX000");
                }
                return invoke(m, c, a);
            });
        });
    }

    private interface Handler {
        Object handle(Method method, Object[] args) throws Throwable;
    }

    private static <T> T proxy(Class<T> type, T target, Handler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (p, method, args) -> handler.handle(method, args)));
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
