package io.github.dsudomoin.outbox.testing;

import io.github.dsudomoin.outbox.internal.TableNames;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** One-statement JDBC helpers for assertions. */
public final class TestSql {

    private TestSql() {}

    /** {@code ob_} + 10 random lowercase hex digits. */
    public static String uniqueTable() {
        return String.format("ob_%010x", ThreadLocalRandom.current().nextLong(1L << 40));
    }

    public static void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    public static long queryLong(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement(); var rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** The first column of every row of {@code sql}, in order. */
    public static List<String> queryStrings(Connection c, String sql, String... args) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setString(i + 1, args[i]);
            }
            try (var rs = ps.executeQuery()) {
                var all = new ArrayList<String>();
                while (rs.next()) {
                    all.add(rs.getString(1));
                }
                return all;
            }
        }
    }

    public static String queryString(Connection c, String sql, String... args) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setString(i + 1, args[i]);
            }
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    /**
     * Terminates the backend holding {@code shard}'s advisory lock: the connection of the step working on it, which
     * holds the lock until its commit. Returns how many it terminated.
     */
    public static int terminateStep(Connection c, int internalNamespace, int shard) throws SQLException {
        try (var ps = c.prepareStatement("select pg_terminate_backend(pid, 5000) from pg_locks where locktype ="
                + " 'advisory' and classid = ?::int4::oid and objid = ?::int4::oid and objsubid = 2 and granted")) {
            ps.setInt(1, internalNamespace);
            ps.setInt(2, TableNames.LOCK_SHARD_BASE + shard);
            int terminated = 0;
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    terminated += rs.getBoolean(1) ? 1 : 0;
                }
            }
            return terminated;
        }
    }
}
