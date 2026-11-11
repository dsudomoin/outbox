package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.OutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/** Dead rows as {@link DeadLetter}s and their redrive: what {@link DeadLetterPass} and {@link PgOutboxAdmin} share. */
final class DeadLetters {

    /** The columns {@link #read} maps, in its order. */
    static final String COLUMNS = "id, message_id, topic, key, payload, headers, created_at, attempts,"
            + " failed_at, error, status, redrives, handler_attempts, due_at";

    /** The columns {@link #readRef} maps, in its order: what a redrive needs, the payload and headers left out. */
    static final String REF_COLUMNS = "id, message_id, topic, key, redrives";

    /** A letter with its headers in the stored encoding. */
    record Claimed(DeadLetter letter, @Nullable byte[] headers) {}

    /**
     * A letter as a redrive claims it: its payload and headers stay in the database, copied from there
     * ({@link Resender#insertFrom}), so a chunk of large letters never comes into the heap.
     */
    record Ref(long id, UUID messageId, String topic, @Nullable byte[] key, int redrives) {}

    /** Letters by their key lock ({@link #tryKeyLocks}): ready to redrive, or held by another transaction. */
    record Split(List<Ref> ready, List<Ref> held) {}

    private final Resender resender;
    private final String dead;
    private final String lockTimeout;
    private final String remember;
    private final String delete;

    DeadLetters(TableNames names, OutboxConfig config) {
        this.resender = new Resender(names, config);
        this.dead = names.dead();
        this.lockTimeout = "set local lock_timeout = '" + XidPartitions.lockWait(config.partitions()).toMillis()
                + "ms'; set local idle_in_transaction_session_timeout = '5s'";
        // one row per message, should two of its letters be redriven together
        this.remember = "insert into " + names.redriven() + " (message_id, redrives)"
                + " select m, max(r) from unnest(?::uuid[], ?::int[]) u(m, r) group by m"
                + " on conflict (message_id) do update set redrives = excluded.redrives, at = now()";
        this.delete = "delete from " + names.dead() + " where id = any(?)";
    }

    /** The row at {@code rs}, selected as {@link #COLUMNS}. */
    static Claimed read(ResultSet rs) throws SQLException {
        var headers = rs.getBytes(6);
        var dueAt = rs.getObject(14, OffsetDateTime.class);
        return new Claimed(new DeadLetter(
                rs.getLong(1),
                rs.getObject(2, UUID.class),
                rs.getString(3),
                rs.getBytes(4),
                rs.getBytes(5),
                HeaderCodec.decode(headers),
                rs.getObject(7, OffsetDateTime.class).toInstant(),
                rs.getInt(8),
                rs.getObject(9, OffsetDateTime.class).toInstant(),
                rs.getString(10),
                DeadLetter.Status.valueOf(rs.getString(11).toUpperCase(Locale.ROOT)),
                rs.getInt(12),
                rs.getInt(13),
                dueAt == null ? null : dueAt.toInstant()), headers);
    }

    /** The row at {@code rs}, selected as {@link #REF_COLUMNS}. */
    static Ref readRef(ResultSet rs) throws SQLException {
        return new Ref(rs.getLong(1), rs.getObject(2, UUID.class), rs.getString(3), rs.getBytes(4), rs.getInt(5));
    }

    /**
     * Tries the key lock of each distinct key without waiting ({@code pg_try_advisory_xact_lock}, held until
     * {@code tx} ends): the unkeyed letters and those whose lock {@code tx} now holds are ready, those whose key
     * another transaction holds, an open {@code send} of it, are held.
     */
    Split tryKeyLocks(Connection tx, List<Ref> letters) throws SQLException {
        return tryKeyLocks(tx, letters, new HashSet<>());
    }

    /**
     * {@link #tryKeyLocks(Connection, List)} across the chunks of one call: the letters of a key in {@code held},
     * found held in an earlier chunk, are held without trying the lock again, and the keys found held now join it.
     * So a key skipped once is skipped for the whole call and comes back whole, in id order, on a later one.
     */
    Split tryKeyLocks(Connection tx, List<Ref> letters, Set<Integer> held) throws SQLException {
        var hashes = letters.stream().map(r -> r.key() == null ? null : Hashing.keyHash(r.topic(), r.key())).toList();
        held.addAll(resender.tryKeyLocks(tx, hashes.stream().filter(h -> h != null && !held.contains(h)).toList()));
        var ready = new ArrayList<Ref>();
        var waiting = new ArrayList<Ref>();
        for (int i = 0; i < letters.size(); i++) {
            (hashes.get(i) != null && held.contains(hashes.get(i)) ? waiting : ready).add(letters.get(i));
        }
        return new Split(ready, waiting);
    }

    /**
     * Back into the outbox, copied from the dead table ({@link Resender#insertFrom}), the redrive counted in {@code <redriven>}, the dead rows deleted. Their
     * key locks are {@code tx}'s already ({@link #tryKeyLocks}), so taken again without waiting; any other lock the
     * insert waits for is waited for at most {@link XidPartitions#lockWait} each.
     */
    void redrive(Connection tx, List<Ref> letters) throws SQLException {
        if (letters.isEmpty()) {
            return;
        }
        limitLockWaits(tx); // after the claim: the insert waits for a lock no longer than the swap
        resender.insertFrom(tx, dead, letters.stream().map(r -> new Resender.Source(r.id(), r.topic(), r.key()))
                .toList());
        try (var ps = tx.prepareStatement(remember)) {
            ps.setArray(1, tx.createArrayOf("uuid", letters.stream().map(Ref::messageId).toArray()));
            ps.setArray(2, tx.createArrayOf("int4", letters.stream().map(r -> r.redrives() + 1).toArray()));
            ps.executeUpdate();
        }
        delete(tx, letters.stream().map(Ref::id).toList());
    }

    /**
     * Each lock wait of the rest of {@code tx} at most {@link XidPartitions#lockWait}, past a swap's hold, and its
     * idle time at most 5 s: no hook runs inside a redrive, so a session cut while it holds key locks keeps the
     * writers of those keys waiting no longer than that.
     */
    void limitLockWaits(Connection tx) throws SQLException {
        try (var st = tx.createStatement()) {
            st.execute(lockTimeout);
        }
    }

    void delete(Connection tx, List<Long> ids) throws SQLException {
        byIds(tx, delete, ids);
    }

    /** {@code sql} with the one parameter {@code ids}, a bigint array; nothing when there are none. */
    static void byIds(Connection tx, String sql, List<Long> ids) throws SQLException {
        if (ids.isEmpty()) {
            return;
        }
        try (var ps = tx.prepareStatement(sql)) {
            ps.setArray(1, tx.createArrayOf("bigint", ids.toArray()));
            ps.executeUpdate();
        }
    }
}
