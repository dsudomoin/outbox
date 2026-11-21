package io.github.dsudomoin.outbox.internal;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** JDBC callbacks that may throw {@link SQLException}, the one-statement {@link #execute} and {@link #queryLong}. */
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
