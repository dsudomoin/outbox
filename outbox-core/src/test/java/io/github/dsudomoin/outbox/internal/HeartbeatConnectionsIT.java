package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntPredicate;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/** The heartbeat's own connections, the application's as the fallback, and the check that both reach one database. */
class HeartbeatConnectionsIT {

    private static final String OTHER = "d" + TestSql.uniqueTable();
    private static final String TABLE = "public." + TestSql.uniqueTable();
    private static final DataSource MAIN = TestDataSources.newDataSource();

    @BeforeAll
    static void databases() throws SQLException {
        try (var c = MAIN.getConnection()) {
            TestSql.execute(c, "create database " + OTHER);
            TestSql.execute(c, "create table " + TABLE + " (id int)");
        }
    }

    @AfterAll
    static void dropThem() throws SQLException {
        try (var c = MAIN.getConnection()) {
            TestSql.execute(c, "drop table " + TABLE);
            TestSql.execute(c, "drop database " + OTHER + " with (force)");
        }
    }

    @Test
    void matchingSourceAtStartServesTheBeats() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> false);
        var connections = new HeartbeatConnections(app, lane);

        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }
        for (int i = 0; i < 3; i++) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(lane.opened.get(i + 1));
            }
        }

        assertThat(lane.calls).isEqualTo(4);
        assertThat(app.calls).isZero();
        assertThat(lane.opened.get(0).isClosed()).as("the check's connection").isTrue();
    }

    @Test
    void otherDatabaseAtStartFailsIt() throws Exception {
        var lane = new Recorded("lane", other(), n -> false);
        var connections = new HeartbeatConnections(new Recorded("app", MAIN, n -> false), lane);

        try (var reference = MAIN.getConnection()) {
            assertThatThrownBy(() -> connections.start(reference, TABLE)).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("reaches another database than the application's")
                    .hasMessageContaining("database " + OTHER)
                    .hasMessageContaining("database " + reference.getCatalog());
        }
        assertThat(lane.opened.get(0).isClosed()).isTrue();
    }

    @Test
    void unreachableAtStartFallsBackWithOneWarn() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> true);
        var connections = new HeartbeatConnections(app, lane);

        try (var log = new CapturedStderr()) {
            try (var reference = MAIN.getConnection()) {
                connections.start(reference, TABLE);
            }
            for (int i = 0; i < 2; i++) {
                try (var c = connections.newConnection()) {
                    assertThat(c).isSameAs(app.opened.get(i));
                }
            }

            assertThat(app.calls).isEqualTo(2);
            assertThat(log.text()).containsOnlyOnce("unreachable at start")
                    .containsOnlyOnce("this beat uses the application's connections");
        }
    }

    @Test
    void deferredCheckMeasuresBothSidesAtOnce() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", MAIN, n -> n == 1);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(0));
        }
        assertThat(app.calls).isEqualTo(1);
        assertThat(app.opened.get(0).isClosed()).as("the comparison's connection").isTrue();
        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(1));
        }
        assertThat(app.calls).isEqualTo(1);
    }

    @Test
    void deferredMismatchTurnsTheLaneOff() throws Exception {
        var app = new Recorded("app", MAIN, n -> false);
        var lane = new Recorded("lane", other(), n -> n == 1);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        try (var log = new CapturedStderr()) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(app.opened.get(0));
            }
            assertThat(log.text()).containsOnlyOnce("reaches another database");
        }
        assertThat(lane.opened.get(0).isClosed()).isTrue();
        for (int i = 1; i < 3; i++) {
            try (var c = connections.newConnection()) {
                assertThat(c).isSameAs(app.opened.get(i));
            }
        }
        assertThat(lane.calls).isEqualTo(2);
    }

    @Test
    void deferredCheckWithoutAnAppConnectionFailsTheBeat() throws Exception {
        var app = new Recorded("app", MAIN, n -> n == 1);
        var lane = new Recorded("lane", MAIN, n -> n == 1);
        var connections = new HeartbeatConnections(app, lane);
        try (var reference = MAIN.getConnection()) {
            connections.start(reference, TABLE);
        }

        assertThatThrownBy(connections::newConnection).isInstanceOf(SQLException.class).hasMessage("app refused");
        assertThat(lane.opened.get(0).isClosed()).isTrue();
        try (var c = connections.newConnection()) {
            assertThat(c).isSameAs(lane.opened.get(1));
        }
        assertThat(app.opened.get(0).isClosed()).isTrue();
    }

    private static DataSource other() {
        var ds = (PGSimpleDataSource) TestDataSources.newDataSource();
        ds.setDatabaseName(OTHER);
        return ds;
    }

    /** Counts its calls and keeps what it handed out; call {@code n} (from 1) throws when {@code fails} says so. */
    private static final class Recorded implements ConnectionSource {

        final List<Connection> opened = new ArrayList<>();
        int calls;
        private final String name;
        private final DataSource source;
        private final IntPredicate fails;

        Recorded(String name, DataSource source, IntPredicate fails) {
            this.name = name;
            this.source = source;
            this.fails = fails;
        }

        @Override
        public Connection newConnection() throws SQLException {
            if (fails.test(++calls)) {
                throw new SQLException(name + " refused");
            }
            var c = source.getConnection();
            opened.add(c);
            return c;
        }
    }
}
