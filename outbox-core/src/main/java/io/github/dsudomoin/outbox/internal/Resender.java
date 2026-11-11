package io.github.dsudomoin.outbox.internal;

import io.github.dsudomoin.outbox.OutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * The bulk inserts into the outbox: {@code sendAll} ({@link #insertAll}), and every path that puts rows back into it
 * (orphan restore, redrive, stranded-shard repair: {@link #insertFrom}). A row gets a new id and the inserting
 * transaction's xid; it keeps its {@code message_id} and stored headers, and its shard is recomputed for the current
 * {@code outbox.shards}. Keyed rows take {@code pg_advisory_xact_lock(keyNamespace, keyHash)}, the lock {@code send}
 * takes, before the identity assigns their ids, so a row cannot interleave with a concurrent {@code send} of its
 * (topic, key).
 */
public final class Resender {

    /**
     * One row whatever the driver's fetch size: a row-per-lock result read in portals (pgjdbc
     * {@code defaultRowFetchSize} in a transaction) would take only the first portal's locks. The planner keeps the
     * volatile lock call in the unused subquery column and does not pull the sorted subquery up, so every key is
     * locked, in ascending hash order.
     */
    private static final String LOCK_ALL = "select count(*) from (select pg_advisory_xact_lock(?, h)"
            + " from (select distinct unnest(?::int[]) as h order by 1) s) l";
    /** One row whatever the fetch size too, so every hash is tried; never waits. */
    private static final String TRY_LOCKS = "select array_agg(h) filter (where not pg_try_advisory_xact_lock(?, h))"
            + " from unnest(?::int[]) h";

    /**
     * The bound on the key, payload and header bytes of one {@link #insertAll} statement: a Bind message, every array
     * of the statement in it, may carry at most 1 GB, and the driver encodes the arrays in the heap first.
     */
    static final long STATEMENT_BYTES = 64L << 20;

    /** A row to insert; {@code headers} in the stored encoding ({@link HeaderCodec}). */
    public record Row(
            UUID messageId, String topic, @Nullable byte[] key, @Nullable byte[] payload, @Nullable byte[] headers) {}

    /** A row of another table to copy ({@link #insertFrom}): its id there, and the topic and key it has there. */
    public record Source(long id, String topic, @Nullable byte[] key) {}

    private final int keyNamespace;
    private final TableNames names;
    private final int shards;
    private final ShardGuard guard;
    private final String insertAll;
    private final long statementBytes;

    /**
     * Without the instance's {@link ShardGuard}: for tests. Main code builds it from the resolved config
     * ({@link #Resender(TableNames, OutboxConfig)}) or, in {@link PgOutbox}, with the guard itself.
     */
    public Resender(TableNames names, int shards) {
        this(names, shards, ShardGuard.NONE);
    }

    /** {@code config}: the instance's, as {@link OutboxSchema#config()} resolved it; every insert checks its guard. */
    public Resender(TableNames names, OutboxConfig config) {
        this(names, config.shards(), ShardGuard.of(config));
    }

    Resender(TableNames names, int shards, ShardGuard guard) {
        this(names, shards, guard, STATEMENT_BYTES);
    }

    /** {@code statementBytes}: as {@link #STATEMENT_BYTES}, smaller in tests. */
    Resender(TableNames names, int shards, ShardGuard guard, long statementBytes) {
        this.names = names;
        this.statementBytes = statementBytes;
        this.guard = guard;
        this.keyNamespace = names.keyNamespace();
        this.shards = shards;
        this.insertAll = "insert into " + names.base() + " (shard, message_id, topic, key, payload, headers)"
                + " select u.shard, u.mid, u.topic, u.key, u.payload, u.headers"
                + " from unnest(?::smallint[], ?::uuid[], ?::text[], ?::bytea[], ?::bytea[], ?::bytea[])"
                + " with ordinality u(shard, mid, topic, key, payload, headers, ord) order by u.ord";
    }

    /**
     * Tries the key lock of each distinct hash in {@code hashes} (nulls skipped) without waiting, held until
     * {@code inTx} ends; returns the hashes another transaction holds, an open {@code send} of the key. A row whose
     * hash is not among them can go through {@link #insertAll} without waiting for its key.
     */
    public Set<Integer> tryKeyLocks(Connection inTx, Collection<Integer> hashes) throws SQLException {
        var distinct = hashes.stream().filter(Objects::nonNull).distinct().toArray(Integer[]::new);
        if (distinct.length == 0) {
            return Set.of();
        }
        try (var ps = inTx.prepareStatement(TRY_LOCKS)) {
            ps.setInt(1, keyNamespace);
            ps.setArray(2, inTx.createArrayOf("int4", distinct));
            try (var rs = ps.executeQuery()) {
                rs.next();
                var held = rs.getArray(1);
                return held == null ? Set.of() : Set.of((Integer[]) held.getArray());
            }
        }
    }

    /**
     * Locks the rows' distinct key hashes in one statement, then inserts them, ids in list order: in one statement, or
     * in consecutive ones of at most {@link #STATEMENT_BYTES} of keys, payloads and headers each when the list holds
     * more (a row over it in one of its own). Unkeyed rows take no lock and land on a random shard. Every key lock is
     * held until {@code inTx} ends, so callers bound the list. Returns the rows inserted. Throws
     * {@link IllegalStateException} while the instance's {@link ShardGuard} finds the cursors on another shard count.
     */
    public int insertAll(Connection inTx, List<Row> rows) throws SQLException {
        guard.require(names, shards);
        int n = rows.size();
        if (n == 0) {
            return 0;
        }
        var ids = new UUID[n];
        var topics = new String[n];
        var keys = new byte[n][];
        var payloads = new byte[n][];
        var headers = new byte[n][];
        for (int i = 0; i < n; i++) {
            var r = rows.get(i);
            ids[i] = r.messageId();
            topics[i] = r.topic();
            keys[i] = r.key();
            payloads[i] = r.payload();
            headers[i] = r.headers();
        }
        var rowShards = lockKeys(inTx, topics, keys);
        int inserted = 0;
        for (int from = 0, to; from < n; from = to) {
            long bytes = 0;
            for (to = from; to < n; to++) {
                long size = length(keys[to]) + length(payloads[to]) + length(headers[to]);
                if (to > from && bytes + size > statementBytes) {
                    break;
                }
                bytes += size;
            }
            try (var ps = inTx.prepareStatement(insertAll)) {
                ps.setArray(1, inTx.createArrayOf("int2", slice(rowShards, from, to)));
                ps.setArray(2, inTx.createArrayOf("uuid", slice(ids, from, to)));
                ps.setArray(3, inTx.createArrayOf("text", slice(topics, from, to)));
                ps.setArray(4, inTx.createArrayOf("bytea", slice(keys, from, to)));
                ps.setArray(5, inTx.createArrayOf("bytea", slice(payloads, from, to)));
                ps.setArray(6, inTx.createArrayOf("bytea", slice(headers, from, to)));
                inserted += ps.executeUpdate();
            }
        }
        return inserted;
    }

    private static long length(@Nullable byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }

    /** {@code a} itself when the range is all of it, the common case of a list under the bound. */
    private static <T> T[] slice(T[] a, int from, int to) {
        return from == 0 && to == a.length ? a : Arrays.copyOfRange(a, from, to);
    }

    /**
     * As {@link #insertAll}, but each row copied from its row in {@code from} inside the database: message id, topic,
     * key, payload and headers, so no payload comes into Java, however large a chunk's. {@code from} is a table, or a
     * parenthesised select, with those columns and an {@code id} unique in it; the rows' topics and keys are those it
     * holds, as their key locks and shards are computed from them here. A source row no longer there is skipped.
     * Returns the rows inserted.
     */
    public int insertFrom(Connection inTx, String from, List<Source> rows) throws SQLException {
        guard.require(names, shards);
        int n = rows.size();
        if (n == 0) {
            return 0;
        }
        var ids = new Long[n];
        var topics = new String[n];
        var keys = new byte[n][];
        for (int i = 0; i < n; i++) {
            var r = rows.get(i);
            ids[i] = r.id();
            topics[i] = r.topic();
            keys[i] = r.key();
        }
        var rowShards = lockKeys(inTx, topics, keys);
        try (var ps = inTx.prepareStatement("insert into " + names.base()
                + " (shard, message_id, topic, key, payload, headers)"
                + " select u.shard, s.message_id, s.topic, s.key, s.payload, s.headers"
                + " from unnest(?::bigint[], ?::smallint[]) with ordinality u(id, shard, ord)"
                + " join " + from + " s on s.id = u.id order by u.ord")) {
            ps.setArray(1, inTx.createArrayOf("int8", ids));
            ps.setArray(2, inTx.createArrayOf("int2", rowShards));
            return ps.executeUpdate();
        }
    }

    /**
     * The shard of each row, recomputed for the current {@code outbox.shards}, a random one when it has no key; the
     * keyed rows' distinct key hashes locked in one statement, until {@code inTx} ends.
     */
    private Short[] lockKeys(Connection inTx, String[] topics, byte[][] keys) throws SQLException {
        int n = topics.length;
        var rowShards = new Short[n];
        var hashes = new Integer[n];
        int keyed = 0;
        for (int i = 0; i < n; i++) {
            if (keys[i] == null) {
                rowShards[i] = (short) Hashing.randomShard(shards);
            } else {
                int hash = Hashing.keyHash(topics[i], keys[i]);
                hashes[keyed++] = hash;
                rowShards[i] = (short) Hashing.shard(hash, shards);
            }
        }
        if (keyed > 0) {
            try (var ps = inTx.prepareStatement(LOCK_ALL)) { // distinct and sorted: no deadlock between bulk writers
                ps.setInt(1, keyNamespace);
                ps.setArray(2, inTx.createArrayOf("int4", Arrays.copyOf(hashes, keyed)));
                ps.execute();
            }
        }
        return rowShards;
    }
}
