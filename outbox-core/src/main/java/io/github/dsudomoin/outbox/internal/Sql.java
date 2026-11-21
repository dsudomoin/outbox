package io.github.dsudomoin.outbox.internal;

import java.sql.Connection;
import java.sql.SQLException;

/** JDBC callbacks that may throw {@link SQLException}, and the one-statement {@link #execute}. */
public final class Sql {

    private Sql() {}

    /** Runs {@code sql} on {@code c} in a statement of its own. */
    public static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
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
