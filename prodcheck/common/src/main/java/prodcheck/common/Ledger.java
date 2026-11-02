package prodcheck.common;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

/** The ledger's writes (DDL: {@code scenarios/ledger.sql}), each on the caller's transaction. */
public final class Ledger {

    public record Sent(UUID id, String app, String instance, String outbox, String topic, @Nullable String key,
            long seq, String tag, int contract) {}

    private Ledger() {}

    public static long nextSeq(Connection c, String topic, String key) throws SQLException {
        try (var ps = c.prepareStatement("""
                insert into pc_key_seq (topic, key, seq) values (?, ?, 1)
                on conflict (topic, key) do update set seq = pc_key_seq.seq + 1 returning seq""")) {
            ps.setString(1, topic);
            ps.setString(2, key);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    /** Takes {@code n} seqs of the key at once; the last one. */
    public static long nextSeqs(Connection c, String topic, String key, int n) throws SQLException {
        try (var ps = c.prepareStatement("""
                insert into pc_key_seq (topic, key, seq) values (?, ?, ?)
                on conflict (topic, key) do update set seq = pc_key_seq.seq + excluded.seq returning seq""")) {
            ps.setString(1, topic);
            ps.setString(2, key);
            ps.setLong(3, n);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    public static void sent(Connection c, List<Sent> rows) throws SQLException {
        try (var ps = c.prepareStatement("""
                insert into pc_sent (message_id, app, instance, outbox, topic, key, seq, tag, contract)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)""")) {
            for (var row : rows) {
                ps.setObject(1, row.id());
                ps.setString(2, row.app());
                ps.setString(3, row.instance());
                ps.setString(4, row.outbox());
                ps.setString(5, row.topic());
                ps.setString(6, row.key());
                ps.setLong(7, row.seq());
                ps.setString(8, row.tag());
                ps.setInt(9, row.contract());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public static void rolledBack(Connection c, List<Sent> rows) throws SQLException {
        try (var ps = c.prepareStatement("insert into pc_rolled_back (message_id, topic, key) values (?, ?, ?)")) {
            for (var row : rows) {
                ps.setObject(1, row.id());
                ps.setString(2, row.topic());
                ps.setString(3, row.key());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    public static void arrival(Connection c, Event e) throws SQLException {
        try (var ps = c.prepareStatement("""
                insert into pc_arrival (sink, message_id, topic, key, seq, tag, instance, contract)
                values (?, ?, ?, ?, ?, ?, ?, ?)""")) {
            ps.setString(1, e.sink());
            ps.setObject(2, e.id());
            ps.setString(3, e.topic());
            ps.setString(4, e.key());
            ps.setLong(5, e.seq());
            ps.setString(6, e.tag());
            ps.setString(7, e.instance());
            ps.setInt(8, e.contract());
            ps.executeUpdate();
        }
    }

    public static void effect(Connection c, Event e) throws SQLException {
        try (var ps = c.prepareStatement("""
                insert into pc_effect (sink, message_id, topic, key, seq, tag, contract)
                values (?, ?, ?, ?, ?, ?, ?)""")) {
            ps.setString(1, e.sink());
            ps.setObject(2, e.id());
            ps.setString(3, e.topic());
            ps.setString(4, e.key());
            ps.setLong(5, e.seq());
            ps.setString(6, e.tag());
            ps.setInt(7, e.contract());
            ps.executeUpdate();
        }
    }

    /** True when the id is new here; on the DB route it decides the effect. */
    public static boolean dedupe(Connection c, UUID id) throws SQLException {
        try (var ps = c.prepareStatement("insert into pc_dedupe (message_id) values (?) on conflict do nothing")) {
            ps.setObject(1, id);
            return ps.executeUpdate() == 1;
        }
    }
}
