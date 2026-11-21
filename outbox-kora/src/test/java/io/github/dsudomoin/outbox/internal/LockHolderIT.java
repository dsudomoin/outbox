package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestPooler;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@link LockHolder} behind pgbouncer in transaction mode: it holds what it took for as long as it is alive, lets go
 * once Postgres ends it, and fences off the work under it then; nothing is left in any server session.
 */
class LockHolderIT {

    private static TestPooler pooler;

    private final int ns = ThreadLocalRandom.current().nextInt();
    private final TestDatabase db = TestDb.newDatabase(pooler, 8);
    private final List<AutoCloseable> closing = new ArrayList<>();

    @BeforeAll
    static void startPooler() throws SQLException {
        pooler = TestPooler.start(4, 0);
        pooler.warm();
    }

    @AfterAll
    static void stopPooler() {
        pooler.close();
    }

    @AfterEach
    void nothingLeftInTheServerSessions() throws Exception {
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
        db.release();
        assertThat(pooler.leftovers()).isEmpty();
    }

    @Test
    void holdsUntilClosedAndReleasesOnlyWhatItTookLast() throws Exception {
        var a = open(Duration.ofSeconds(60));
        var b = open(Duration.ofSeconds(60));

        assertThat(a.lock(ns, 1, Duration.ZERO)).isNotNull();
        var second = a.lock(ns, 2, Duration.ZERO);
        assertThat(b.lock(ns, 1, Duration.ZERO)).as("held").isNull();
        assertThat(b.lock(ns, 2, Duration.ofMillis(200))).as("held, waited for").isNull();
        a.release(second);
        assertThat(b.lock(ns, 2, Duration.ZERO)).as("released").isNotNull();
        assertThat(b.lock(ns, 1, Duration.ZERO)).as("still held").isNull();
        a.close();
        assertThat(b.lock(ns, 1, Duration.ofSeconds(5))).as("closed").isNotNull();
    }

    @Test
    void anIdleHolderHoldsNoSnapshotNorTransactionId() throws Exception {
        var a = LockHolder.open(db, new SocketBound(Duration.ZERO), Duration.ofSeconds(60), Duration.ofMillis(100));
        closing.add(a);
        a.lock(ns, 1, Duration.ZERO);
        var section = a.lock(ns, 2, Duration.ofSeconds(1));
        assertThat(idleHolder(1)).as("after its locks").isEqualTo("idle in transaction|null|null");
        a.release(section);
        assertThat(idleHolder(1)).as("after a release").isEqualTo("idle in transaction|null|null");

        Thread.sleep(500); // its keepalive's statements

        assertThat(idleHolder(1)).as("after keepalives").isEqualTo("idle in transaction|null|null");
    }

    @Test
    void theKeepaliveHoldsAnIdleHolderPastItsIdleTimeout() throws Exception {
        var a = LockHolder.open(db, new SocketBound(Duration.ZERO), Duration.ofSeconds(1), Duration.ofMillis(200));
        closing.add(a);
        a.lock(ns, 1, Duration.ZERO);

        Thread.sleep(2500);

        assertThat(a.holds()).isTrue();
        assertThat(open(Duration.ofSeconds(60)).lock(ns, 1, Duration.ZERO)).isNull();
    }

    @Test
    void aSilentHolderLosesItsLocksAfterItsIdleTimeout() throws Exception {
        var a = LockHolder.open(db, new SocketBound(Duration.ZERO), Duration.ofSeconds(1), Duration.ofSeconds(30)); // as if frozen
        closing.add(a);
        a.lock(ns, 1, Duration.ZERO);

        assertThat(open(Duration.ofSeconds(60)).lock(ns, 1, Duration.ofSeconds(10))).as("Postgres ended it")
                .isNotNull();
    }

    @Test
    void aHolderPostgresEndedFencesItsWorkOff() throws Exception {
        var a = open(Duration.ofSeconds(60));
        a.lock(ns, 1, Duration.ZERO);
        try (var work = db.newConnection()) {
            a.fence(work); // alive

            terminateHolderOf(1);
            assertThat(open(Duration.ofSeconds(60)).lock(ns, 1, Duration.ofSeconds(10))).as("free for another")
                    .isNotNull();

            assertThatThrownBy(() -> a.fence(work)).isInstanceOf(SQLException.class)
                    .extracting(e -> ((SQLException) e).getSQLState()).isEqualTo(LockHolder.LOST);
            work.setAutoCommit(false);
            assertThatThrownBy(() -> a.fence(work)).as("in a transaction too").isInstanceOf(SQLException.class);
            work.rollback();
            work.setAutoCommit(true);
        }
    }

    @Test
    void aLostHolderCancelsTheLongStatementOfItsWork() throws Exception {
        var a = LockHolder.open(db, new SocketBound(Duration.ZERO), Duration.ofSeconds(60), Duration.ofMillis(200));
        closing.add(a);
        a.lock(ns, 1, Duration.ZERO);
        var work = db.newConnection();
        closing.add(() -> { // aborted by the holder's loss: a pool's proxy throws on its close
            try {
                work.close();
            } catch (SQLException e) {
                // gone already
            }
        });
        var ran = CompletableFuture.runAsync(() -> {
            try (var scope = a.bind(work)) {
                PartitionDdl.executeConcurrently(work, Duration.ZERO, "select pg_sleep(60)", null);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(500); // running

        terminateHolderOf(1);

        assertThatThrownBy(() -> ran.get(10, TimeUnit.SECONDS)).as("cancelled within the keepalive's period")
                .hasCauseInstanceOf(RuntimeException.class);
        assertThat(a.holds()).isFalse();
    }

    @Test
    void theLeftoverCheckSeesASessionSettingAndLock() throws Exception {
        try (var c = pooler.connect()) {
            TestSql.execute(c, "set lock_timeout = 1234");
            TestSql.execute(c, "select pg_advisory_lock(" + ns + ", 7)");
        }
        try {
            assertThat(pooler.leftovers())
                    .anyMatch(l -> l.contains("lock_timeout = 1234"))
                    .anyMatch(l -> l.contains("advisory lock (" + Integer.toUnsignedLong(ns) + ", 7, 2)"));
        } finally {
            pooler.scrub();
        }
    }

    private LockHolder open(Duration idle) throws SQLException {
        var holder = LockHolder.open(db, new SocketBound(Duration.ZERO), idle);
        closing.add(holder);
        return holder;
    }

    /** The state, xmin and xid of the server session that holds {@code (ns, key)}. */
    private String idleHolder(int key) throws SQLException {
        try (Connection c = TestDb.connect()) {
            return TestSql.queryString(c, "select a.state || '|' || coalesce(a.backend_xmin::text, 'null') || '|'"
                    + " || coalesce(a.backend_xid::text, 'null') from pg_locks l join pg_stat_activity a using (pid)"
                    + " where l.locktype = 'advisory' and l.classid::bigint = " + Integer.toUnsignedLong(ns)
                    + " and l.objid = " + key + " and l.objsubid = 2 and l.granted");
        }
    }

    /** Ends the server session that holds {@code (ns, key)}, as Postgres ends a holder past its idle timeout. */
    private void terminateHolderOf(int key) throws SQLException {
        try (Connection c = TestDb.connect()) {
            assertThat(TestSql.queryLong(c, "select count(pg_terminate_backend(pid)) from pg_locks where"
                    + " locktype = 'advisory' and classid::bigint = " + Integer.toUnsignedLong(ns) + " and objid = "
                    + key + " and objsubid = 2 and granted")).isOne();
        }
    }
}
