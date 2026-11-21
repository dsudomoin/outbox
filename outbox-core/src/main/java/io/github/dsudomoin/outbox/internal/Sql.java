package io.github.dsudomoin.outbox.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * JDBC callbacks that may throw {@link SQLException}, the one-statement {@link #execute} and {@link #queryLong}, and
 * {@link #inTransaction}.
 */
public final class Sql {

    private Sql() {}

    /** Runs {@code sql} on {@code c} in a statement of its own. */
    public static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    /** The first column of the first row {@code sql} returns on {@code st}, 0 when it is null. */
    public static long queryLong(Statement st, String sql) throws SQLException {
        try (var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /**
     * {@code body} in a transaction on the autocommit connection {@code c}, given back in autocommit: commits after
     * {@code body}, rolls back when it throws.
     */
    static <T> T inTransaction(Connection c, SqlFunction0<T> body) throws SQLException {
        c.setAutoCommit(false);
        T result;
        try {
            result = body.apply();
            c.commit();
        } catch (Throwable e) { // an Error too: setAutoCommit(true) would commit the work so far
            try {
                c.rollback();
                c.setAutoCommit(true);
            } catch (SQLException | RuntimeException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        c.setAutoCommit(true);
        return result;
    }

    @FunctionalInterface
    public interface SqlFunction0<T> {
        T apply() throws SQLException;
    }

    @FunctionalInterface
    public interface SqlFunction1<A, T> {
        T apply(A a) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlConsumer<A> {
        void accept(A a) throws SQLException;
    }

    @FunctionalInterface
    public interface SqlRunnable {
        void run() throws SQLException;
    }
}
