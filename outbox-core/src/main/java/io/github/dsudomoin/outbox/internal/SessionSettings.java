package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.internal.Sql.SqlFunction0;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Settings this library sets on an autocommit session for its own work, given back after it: the session's own value
 * where the session set one (as a pool's {@code connectionInitSql} does), else a {@code RESET} to the role, database
 * or server default. A bare {@code RESET} would drop the session's own value, and the pool would hand the session out
 * without it. When giving them back fails, the connection is aborted, so no pool hands it out again with this
 * library's values.
 */
final class SessionSettings {

    /** By setting name: the session's own value, null where it set none. */
    private final Map<String, String> saved;

    private SessionSettings(Map<String, String> saved) {
        this.saved = saved;
    }

    /** Saves the session's own values of {@code settings}' names, then sets each to its SQL value. */
    static SessionSettings set(Connection c, Map<String, String> settings) throws SQLException {
        var saved = new HashMap<String, String>();
        settings.keySet().forEach(name -> saved.put(name, null));
        try (var ps = c.prepareStatement("select name, setting from pg_settings where name = any(?)"
                + " and source = 'session'")) {
            ps.setArray(1, c.createArrayOf("text", settings.keySet().toArray()));
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    saved.put(rs.getString(1), rs.getString(2));
                }
            }
        }
        var session = new SessionSettings(saved);
        try (var st = c.createStatement()) {
            for (var e : settings.entrySet()) {
                st.execute("set " + e.getKey() + " = " + e.getValue());
            }
        } catch (SQLException | RuntimeException e) {
            session.restore(c, e);
            throw e;
        }
        return session;
    }

    /**
     * {@code settings} with {@code transaction_timeout} off where the server has it (17+): for work that bounds its own
     * waits, which a shorter role or database default would end with the session (FATAL 25P04), not a 55P03 or 57014.
     */
    static Map<String, String> noTransactionTimeout(Connection c, Map<String, String> settings) throws SQLException {
        if (c.getMetaData().getDatabaseMajorVersion() < 17) {
            return settings;
        }
        var off = new HashMap<>(settings);
        off.put("transaction_timeout", "0");
        return off;
    }

    /** {@code body} under {@code settings} ({@link #set}), given back on every path. */
    static <T> T with(Connection c, Map<String, String> settings, SqlFunction0<T> body) throws SQLException {
        var session = set(c, settings);
        T result;
        try {
            result = body.apply();
        } catch (Throwable e) {
            session.restore(c, e);
            throw e;
        }
        session.restore(c);
        return result;
    }

    /** Gives the saved values back, and when that fails aborts {@code c} and throws. */
    void restore(Connection c) throws SQLException {
        try {
            for (var e : saved.entrySet()) {
                if (e.getValue() == null) {
                    try (var st = c.createStatement()) {
                        st.execute("reset " + e.getKey());
                    }
                } else {
                    try (var ps = c.prepareStatement("select set_config(?, ?, false)")) {
                        ps.setString(1, e.getKey());
                        ps.setString(2, e.getValue());
                        ps.execute();
                    }
                }
            }
        } catch (SQLException | RuntimeException e) {
            try {
                c.abort(Runnable::run);
            } catch (SQLException | RuntimeException aborting) {
                e.addSuppressed(aborting);
            }
            throw e;
        }
    }

    /** {@link #restore} after {@code failure}, which stays on top: a failure here is suppressed into it. */
    private void restore(Connection c, Throwable failure) {
        try {
            restore(c);
        } catch (SQLException | RuntimeException e) {
            failure.addSuppressed(e);
        }
    }
}
