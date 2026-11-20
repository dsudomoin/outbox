package io.github.dsudomoin.outbox.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * A start check for a proxy that keeps no prepared statement across server sessions: pgbouncer in transaction mode
 * before 1.21 or with {@code max_prepared_statements = 0}, Odyssey without {@code pool_reserve_prepared_statement}.
 * Behind one, a statement the driver prepared on the server (pgjdbc does from a statement's
 * {@code prepareThreshold}-th execution on, 5 by default) fails whenever a later execution lands in another server
 * session, SQLState 26000 ("prepared statement ... does not exist") or 42P05, the library's hot statements included.
 * One statement run {@link #RUNS} times on the connection, each in a transaction of its own as the relay's
 * statements run, shows it: past the driver's threshold its runs go to whichever server session the proxy hands out.
 * In autocommit pgjdbc prepares a missing statement again and hides the error, but not inside a transaction, which
 * the error has aborted (unless its {@code autosave} is on, which heals the relay's statements too, so the check then
 * rightly passes). Best effort, and never a false alarm: a quiet pool that keeps handing out one session passes, and
 * the failure then shows later as these SQLStates.
 */
final class PreparedStatements {

    /** Runs of the check's statement: past pgjdbc's default threshold of 5, and a few more for the proxy to rotate. */
    static final int RUNS = 12;
    private static final Set<String> LOST = Set.of("26000", "42P05");

    private PreparedStatements() {}

    /** Fails with {@link IllegalStateException} naming the fix when {@code c}'s prepared statements get lost. */
    static void requireKept(Connection c, String what) throws SQLException {
        boolean autoCommit = c.getAutoCommit();
        try (var ps = c.prepareStatement("select 1")) {
            c.setAutoCommit(false);
            for (int i = 0; i < RUNS; i++) {
                try {
                    ps.executeQuery().close();
                    c.commit();
                } catch (SQLException | RuntimeException e) {
                    try {
                        c.rollback();
                    } catch (SQLException cleanup) {
                        e.addSuppressed(cleanup);
                    }
                    throw e;
                }
            }
        } catch (SQLException e) {
            if (!LOST.contains(e.getSQLState())) {
                throw e;
            }
            throw new IllegalStateException(what + ": a statement the driver prepared on one server session was"
                    + " missing in another (SQLState " + e.getSQLState() + ": " + e.getMessage() + "): the"
                    + " connections go through a transaction-pooling proxy that keeps no prepared statement across"
                    + " server sessions (pgbouncer before 1.21 or with max_prepared_statements = 0, Odyssey without"
                    + " pool_reserve_prepared_statement). Set prepareThreshold=0 on the DataSource (in Kora"
                    + " db.dsProperties.prepareThreshold, in Spring Boot"
                    + " spring.datasource.hikari.data-source-properties.prepareThreshold), or have the proxy keep"
                    + " prepared statements", e);
        } finally {
            c.setAutoCommit(autoCommit);
        }
    }
}
