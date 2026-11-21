package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.testing.TestSql.execute;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static io.github.dsudomoin.outbox.testing.TestSql.queryLongs;
import static io.github.dsudomoin.outbox.testing.TestSql.queryString;
import static io.github.dsudomoin.outbox.testing.TestSql.queryStrings;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RoundTrips;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TicksIT {

    private static final int SHARDS = 8;

    private final TestDatabase db = TestDb.newDatabase(2);
    private TableNames names;
    private Ticks ticks;

    @BeforeEach
    void bootstrap() throws Exception {
        var schema = new OutboxSchema(db, new TestOutboxConfig().shards(SHARDS));
        schema.start();
        names = schema.names();
        ticks = new Ticks(names, SHARDS);
    }

    @AfterEach
    void release() {
        db.release();
    }

    /**
     * A tick costs two round trips: the settings and the tick lock, then the probe, the tick row and the commit; each
     * round trip is a commit's wait for its tick on a network with latency. The young and the regressed attempts end
     * in the same two.
     */
    @Test
    void aTickTakesTwoRoundTrips() throws Exception {
        try (var c = TestDb.connect()) {
            ticks.make(c, Duration.ZERO); // the probe's statement is built once, from the catalog
            long before = ticks.latest(c).id();

            var made = RoundTrips.count(() -> ticks.tick(c, Duration.ZERO));
            assertThat(made.result()).isEqualTo(Ticks.Made.INSERTED);
            assertThat(made.roundTrips()).as("round trips of a tick").isEqualTo(2);
            assertThat(ticks.latest(c).id()).isGreaterThan(before);
            assertThat(c.getAutoCommit()).as("autocommit restored").isTrue();

            var young = RoundTrips.count(() -> ticks.tick(c, Duration.ofHours(1)));
            assertThat(young.result()).isEqualTo(Ticks.Made.SKIPPED);
            assertThat(young.roundTrips()).as("round trips of a skipped tick").isEqualTo(2);
        }
    }

    /**
     * The tick brings the database clock a reading, as the read of the latest tick its maker no longer runs did: a
     * relay of one worker under steady commits makes the tick of every pass, and its clock would otherwise read only at
     * the heartbeat, so a time made after a VM pause stayed off by the pause for that long.
     */
    @Test
    void aTickBringsTheDatabaseClockAReading() throws Exception {
        var clock = new DatabaseClock(java.time.InstantSource.fixed(java.time.Instant.EPOCH)); // a JVM clock far off
        try (var c = TestDb.connect()) {
            assertThat(ticks.attempt(c, Duration.ZERO, clock).made()).isEqualTo(Ticks.Made.INSERTED);
        }
        assertThat(Duration.between(clock.now(), java.time.Instant.now()).abs()).isLessThan(Duration.ofMinutes(1));
    }

    /**
     * The step's cursor read chooses each shard's ticks as {@link Ticks#fresh} and {@code chooseTarget} do, which it
     * replaces: on random tick logs (marking bitmaps of every kind, null bitmaps and {@code max_id}s, gaps in the ids)
     * and random cursors (fresh and mid-batch, below and at the latest tick), the same skipped-to tick and target.
     */
    @Test
    void theCursorReadChoosesTheTicksFreshAndChooseTargetChoose() throws Exception {
        int maxTicks = 5;
        long batchIds = 30;
        var cursors = new Cursors(names, SHARDS);
        var sql = cursors.latestWithTargetsSql(maxTicks, batchIds);
        var random = new Random(7);
        try (var c = TestDb.connect()) {
            long first = ticks.latest(c).id(); // the bootstrap tick
            long maxId = 0;
            for (int i = 0; i < 60; i++) {
                maxId += random.nextInt(12);
                var dirty = switch (random.nextInt(6)) {
                    case 0 -> "null";
                    case 1 -> "''";
                    case 2 -> "B'01'"; // drawn from another shard count: every shard
                    default -> "B'" + IntStream.range(0, SHARDS).mapToObj(b -> random.nextInt(4) == 0 ? "1" : "0")
                            .collect(java.util.stream.Collectors.joining()) + "'";
                };
                var max = random.nextInt(15) == 0 ? "null" : Long.toString(maxId);
                long id = queryLong(c, "insert into " + names.tick() + " (snapshot, max_id, dirty) select"
                        + " pg_current_snapshot(), " + max + ", " + dirty + "::varbit returning id");
                if (random.nextInt(10) == 0 && i < 55) {
                    execute(c, "delete from " + names.tick() + " where id = " + id); // a gap in the ids
                }
            }
            var ids = queryLongs(c, "select id from " + names.tick() + " where id > " + first + " order by id");
            for (int round = 0; round < 40; round++) {
                long latest = ids.get(ids.size() / 2 + random.nextInt(ids.size() - ids.size() / 2));
                for (int shard = 0; shard < SHARDS; shard++) {
                    long at = ids.get(random.nextInt(ids.size()));
                    Long target = null;
                    if (random.nextInt(4) == 0) { // mid-batch, its target above its tick
                        var above = ids.stream().filter(id -> id > at).toList();
                        target = above.isEmpty() ? null : above.get(random.nextInt(above.size()));
                    }
                    cursors.append(c, new Cursors.Cursor(shard, Math.min(at, latest), target, 0, null, 0, null, null));
                }
                var expected = new java.util.HashMap<Integer, List<Long>>();
                for (int shard = 0; shard < SHARDS; shard++) {
                    var current = cursors.latest(c, shard);
                    long eff = current.effectiveTickId();
                    var cursor = current.cursor();
                    Long skip = null;
                    Long target = null;
                    if (cursor.targetTickId() != null) {
                        target = cursor.targetTickId();
                    } else if (latest > eff) {
                        var fresh = ticks.fresh(c, Map.of(shard, eff), latest, maxTicks);
                        var skipped = fresh.skipped().get(shard);
                        long lo = skipped != null ? skipped.id() : eff;
                        Long prevMax = skipped != null ? skipped.maxId() : current.tick().maxId();
                        var candidates = List.copyOf(fresh.ticks().subMap(lo, false, Math.min(latest, lo + maxTicks),
                                true).values());
                        skip = skipped == null ? null : skipped.id();
                        target = candidates.isEmpty() ? ticks.oldestAfter(c, lo).id()
                                : ShardProcessor.chooseTarget(prevMax, candidates, batchIds).id();
                    }
                    expected.put(shard, java.util.Arrays.asList(skip, target));
                }
                var actual = new java.util.HashMap<Integer, List<Long>>();
                try (var ps = c.prepareStatement(sql)) {
                    ps.setArray(1, c.createArrayOf("int4", IntStream.range(0, SHARDS).boxed().toArray()));
                    for (int i = 2; i <= 6; i++) {
                        ps.setLong(i, latest);
                    }
                    try (var rs = ps.executeQuery()) {
                        Cursors.targeted(rs).forEach((shard, t) -> actual.put(shard, java.util.Arrays.asList(
                                t.skipped() == null ? null : t.skipped().id(),
                                t.target() == null ? null : t.target().id())));
                    }
                }
                assertThat(actual).as("round %d, latest %d", round, latest).isEqualTo(expected);
            }
        }
    }

    @Test
    void lastUnmarkedSkipsOnlyTheTicksThatDoNotMarkTheShard() throws Exception {
        try (var c = TestDb.connect()) {
            long p = ticks.latest(c).id(); // the bootstrap tick
            long a = tick(c, "''");
            long b = tick(c, "B'01000000'");
            long n = tick(c, "null");
            long d = tick(c, "B'01'"); // drawn from another shard count: every shard
            long e1 = tick(c, "''");
            long e2 = tick(c, "''");
            long latest = tick(c, "''");

            assertThat(lastUnmarked(c, 0, p, latest)).as("shard 0: first marked by the null tick").isEqualTo(b);
            assertThat(lastUnmarked(c, 1, p, latest)).as("shard 1: by its bit").isEqualTo(a);
            assertThat(lastUnmarked(c, 0, n, latest)).as("from the null tick: by the other length").isNull();
            assertThat(lastUnmarked(c, 0, d, latest)).as("only the latest marks it").isEqualTo(e2);
            assertThat(lastUnmarked(c, 0, p, a)).as("the latest marks it, first after p").isNull();

            execute(c, "delete from " + names.tick() + " where id = " + e1); // by hand: a gap before e2
            assertThat(lastUnmarked(c, 0, d, latest)).as("the tick after the gap marks it").isNull();
        }
    }

    /** {@link Ticks#fresh}'s skipped-to tick id, null for none. */
    @Nullable
    private Long lastUnmarked(Connection c, int shard, long afterId, long latestId) throws Exception {
        var tick = ticks.fresh(c, Map.of(shard, afterId), latestId, 100).skipped().get(shard);
        return tick == null ? null : tick.id();
    }

    @Test
    void freshReadsTheTicksAfterEachShardsSkipUpToItsBound() throws Exception {
        try (var c = TestDb.connect()) {
            long p = ticks.latest(c).id(); // the bootstrap tick
            long a = tick(c, "''");
            long b = tick(c, "B'01000000'"); // marks shard 1
            long d = tick(c, "''");
            long e = tick(c, "''");
            long latest = tick(c, "''");

            var fresh = ticks.fresh(c, Map.of(0, p, 1, p, 2, a), latest, 2);

            assertThat(fresh.skipped()).extractingFromEntries(entry -> entry.getKey() + "@" + entry.getValue().id())
                    .as("shards 0 and 2 up to the latest's predecessor, shard 1 to the tick before b")
                    .containsExactlyInAnyOrder("0@" + e, "1@" + a, "2@" + e);
            assertThat(fresh.skipped().get(1).snapshot()).as("without snapshots").isNull();
            // (e, latest] for shards 0 and 2, (a, a + 2] for shard 1: each tick once
            assertThat(fresh.ticks().keySet()).containsExactly(b, d, latest);
            assertThat(ticks.fresh(c, Map.of(0, latest), latest, 2).ticks()).as("nothing after the latest").isEmpty();
            assertThat(ticks.fresh(c, Map.of(), latest, 2).skipped()).isEmpty();
        }
    }

    /** A tick with {@code dirty} (SQL); returns its id. */
    private long tick(Connection c, String dirty) throws Exception {
        return queryLong(c, "insert into " + names.tick() + " (snapshot, max_id, dirty) select pg_current_snapshot(),"
                + " 0, " + dirty + "::varbit returning id");
    }

    @Test
    void makeRespectsMinInterval() throws Exception {
        try (var c = TestDb.connect()) {
            execute(c, "update " + names.tick() + " set created_at = created_at - interval '1 hour'");

            assertThat(ticks.make(c, Duration.ofSeconds(1))).isTrue();
            assertThat(ticks.make(c, Duration.ofSeconds(1))).isFalse();

            assertThat(queryLong(c, "select count(*) from " + names.tick())).isEqualTo(2);
            assertSessionRestored(c);
        }
    }

    @Test
    void futureCreatedAtIsStale() throws Exception {
        try (var c = TestDb.connect()) {
            execute(c, "update " + names.tick() + " set created_at = now() + interval '1 hour'");

            assertThat(ticks.make(c, Duration.ofSeconds(1))).isTrue();
        }
    }

    @Test
    void ticksAreMonotonicUnderConcurrency() throws Exception {
        var xids = new ConcurrentLinkedQueue<Long>();
        var stop = new AtomicBoolean();
        var pool = Executors.newFixedThreadPool(12);
        try {
            var writers = new ArrayList<Future<?>>();
            for (int i = 0; i < 4; i++) {
                writers.add(pool.submit(() -> {
                    try (var c = TestDb.connect()) {
                        c.setAutoCommit(false);
                        while (!stop.get()) {
                            xids.add(xid(c));
                            execute(c, "insert into " + names.base()
                                    + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
                            c.commit();
                        }
                    }
                    return null;
                }));
            }
            var makers = new ArrayList<Future<Integer>>();
            for (int i = 0; i < 8; i++) {
                makers.add(pool.submit(() -> {
                    int made = 0;
                    try (var c = TestDb.connect()) {
                        for (int j = 0; j < 50; j++) {
                            made += ticks.make(c, Duration.ZERO) ? 1 : 0;
                        }
                    }
                    return made;
                }));
            }
            int made = 0;
            for (var f : makers) {
                made += f.get(60, SECONDS);
            }
            stop.set(true);
            for (var f : writers) {
                f.get(60, SECONDS);
            }
            assertThat(made).isEqualTo(400);
        } finally {
            stop.set(true);
            pool.shutdownNow();
        }

        try (var c = TestDb.connect()) {
            var xmaxes = queryLongs(c, "select pg_snapshot_xmax(snapshot)::text::bigint from " + names.tick()
                    + " order by id");
            assertThat(xmaxes).hasSize(401).isSorted();

            // per consecutive tick pair (k, k+1): writer xids visible in k but not in k+1 (lost), and the reverse
            try (var ps = c.prepareStatement("select"
                    + " count(*) filter (where pg_visible_in_snapshot(x, t.snapshot)"
                    + " and not pg_visible_in_snapshot(x, t.next)),"
                    + " count(*) filter (where not pg_visible_in_snapshot(x, t.snapshot)"
                    + " and pg_visible_in_snapshot(x, t.next))"
                    + " from (select snapshot, lead(snapshot) over (order by id) next from " + names.tick() + ") t,"
                    + " lateral (select v::text::xid8 x from unnest(?::bigint[]) v) w"
                    + " where t.next is not null")) {
                ps.setArray(1, c.createArrayOf("bigint", xids.toArray()));
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    assertThat(rs.getLong(1)).as("xids visible in tick k but not in k+1").isZero();
                    assertThat(rs.getLong(2)).as("xids committed between ticks").isPositive();
                }
            }
        }
    }

    @Test
    void frozenHolderIsReleased() throws Exception {
        var frozen = TestDb.connect();
        try (var c = TestDb.connect()) {
            frozen.setAutoCommit(false);
            ticks.lockForTest(frozen); // then never touched again: idle in transaction
            long start = System.nanoTime();

            assertThat(ticks.make(c, Duration.ZERO)).as("tick lock held by the frozen session").isFalse();
            assertSessionRestored(c);
            boolean made = false;
            while (!made && System.nanoTime() - start < Duration.ofSeconds(8).toNanos()) {
                made = ticks.make(c, Duration.ZERO);
            }

            assertThat(made).isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(8));
            assertThat(frozen.isValid(1)).as("frozen session terminated by Postgres").isFalse();
        } finally {
            try {
                frozen.close();
            } catch (Exception ignored) {
                // its session is gone
            }
        }
    }

    /**
     * A role's {@code statement_timeout} below the tick's 1 s {@code lock_timeout} does not turn a held tick lock into
     * a cancelled statement (57014): the tick is skipped, as with any role.
     */
    @Test
    void aShortRoleStatementTimeoutStillSkipsATickWhoseLockIsHeld() throws Exception {
        var props = new Properties();
        props.setProperty("options", "-c statement_timeout=500"); // a role or URL default
        try (var holder = TestDb.connect(); var c = TestDb.connect(props)) {
            holder.setAutoCommit(false);
            ticks.lockForTest(holder);

            assertThat(ticks.tick(c, Duration.ZERO)).isEqualTo(Ticks.Made.SKIPPED);
            assertSessionRestored(c);
            assertThat(queryString(c, "show statement_timeout")).as("the session's again").isEqualTo("500ms");
            holder.rollback();
            assertThat(ticks.tick(c, Duration.ZERO)).isEqualTo(Ticks.Made.INSERTED);
        }
    }

    @Test
    void tickIsMonotonicEvenWhenSessionDefaultsToRepeatableRead() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        try (var holder = TestDb.connect();
                var maker = TestDb.connect();
                var throttled = TestDb.connect();
                var writer = TestDb.connect();
                var observer = TestDb.connect()) {
            execute(observer, "update " + names.tick() + " set created_at = created_at - interval '1 hour'");
            for (var c : List.of(maker, throttled)) {
                execute(c, "set session characteristics as transaction isolation level repeatable read");
            }
            long makerPid = queryLong(maker, "select pg_backend_pid()");
            long throttledPid = queryLong(throttled, "select pg_backend_pid()");
            holder.setAutoCommit(false);
            ticks.lockForTest(holder);

            // queued in this order behind the holder; both take their lock-statement snapshot now
            var made = pool.submit(() -> ticks.make(maker, Duration.ZERO));
            awaitLockWait(observer, makerPid);
            var madeThrottled = pool.submit(() -> ticks.make(throttled, Duration.ofSeconds(1)));
            awaitLockWait(observer, throttledPid);
            writer.setAutoCommit(false);
            long xid = xid(writer);
            writer.commit();
            holder.commit();

            assertThat(made.get(10, SECONDS)).isTrue();
            try (var ps = observer.prepareStatement("select pg_visible_in_snapshot((?::bigint)::text::xid8, snapshot)"
                    + " from " + names.tick() + " order by id desc limit 1")) {
                ps.setLong(1, xid);
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    assertThat(rs.getBoolean(1)).as("xid committed before the tick lock was granted").isTrue();
                }
            }
            assertThat(madeThrottled.get(10, SECONDS)).as("throttled by the tick made just before").isFalse();
            assertThat(queryLong(observer, "select count(*) from " + names.tick())).isEqualTo(2);
            assertSessionRestored(maker);
            assertThat(queryString(maker, "show transaction_isolation")).isEqualTo("repeatable read");
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * A tick commit the relay reads is the gate for every writer commit it covers, so it is flushed even when the
     * role or database says {@code synchronous_commit = off} (a session default stands in for those here); a stronger
     * setting is left as it is, so a tick never waits for a standby the role chose not to wait for, and never stops
     * waiting for one it did.
     */
    @Test
    void aTickCommitsDurablyWhenTheSessionDefaultIsSynchronousCommitOff() throws Exception {
        try (var maker = TestDb.connect(); var observer = TestDb.connect()) {
            execute(observer, "create table sc_probe (n serial, v text); create function sc_probe() returns trigger"
                    + " language plpgsql as $$ begin insert into sc_probe (v)"
                    + " values (current_setting('synchronous_commit')); return new; end $$;"
                    + " create trigger sc_probe before insert on " + names.tick()
                    + " for each row execute function sc_probe()");
            for (var setting : List.of("off", "on", "remote_apply")) {
                execute(observer, "update " + names.tick() + " set created_at = created_at - interval '1 hour'");
                execute(maker, "set synchronous_commit = " + setting);

                assertThat(ticks.make(maker, Duration.ZERO)).isTrue();
                assertThat(queryString(maker, "show synchronous_commit")).as("transaction-scoped").isEqualTo(setting);
            }
            assertThat(queryStrings(observer, "select v from sc_probe order by n"))
                    .as("the tick transaction's synchronous_commit").containsExactly("local", "on", "remote_apply");
        }
    }

    @Test
    void readsAndDeletesTicks() throws Exception {
        try (var c = TestDb.connect()) {
            var first = ticks.latest(c);
            ticks.make(c, Duration.ZERO);
            var middle = ticks.latest(c);
            ticks.make(c, Duration.ZERO);
            var last = ticks.latest(c);

            assertThat(List.of(first.id(), middle.id(), last.id())).isSorted().doesNotHaveDuplicates();
            assertThat(last.snapshot().xmax()).isGreaterThanOrEqualTo(first.snapshot().xmax());
            assertThat(last.createdAt()).isAfterOrEqualTo(first.createdAt());
            assertThat(ticks.get(c, middle.id())).usingRecursiveComparison().isEqualTo(middle);

            assertThat(ticks.deleteBelow(c, middle.id())).isEqualTo(1);

            assertThatThrownBy(() -> ticks.get(c, first.id())).isInstanceOf(IllegalStateException.class);
            assertThat(ticks.get(c, List.of(first.id(), middle.id(), last.id())))
                    .containsOnlyKeys(middle.id(), last.id());
        }
    }

    @Test
    void getManyReadsSnapshots() throws Exception {
        try (var c = TestDb.connect()) {
            var first = ticks.latest(c);
            var running = TestDb.connect(); // in flight at the tick below: a snapshot with an xip
            running.setAutoCommit(false);
            xid(running);
            xid(c); // a later xid ends, so the running one lies below xmax
            ticks.make(c, Duration.ZERO);
            var withXip = ticks.latest(c);
            running.rollback();
            running.close();
            assertThat(withXip.snapshot().xip()).isNotEmpty();

            var many = ticks.get(c, List.of(first.id(), withXip.id(), withXip.id() + 100));
            assertThat(many).containsOnlyKeys(first.id(), withXip.id());
            assertThat(many.get(first.id())).usingRecursiveComparison().isEqualTo(first);
            assertThat(many.get(withXip.id())).as("as the single get reads it").usingRecursiveComparison()
                    .isEqualTo(ticks.get(c, withXip.id()));
            assertThat(ticks.get(c, List.of())).isEmpty();
        }
    }

    @Test
    void ticksRecordTheIdHighWaterMark() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(ticks.latest(c).maxId()).as("bootstrap tick, no id allocated yet").isZero();

            execute(c, "insert into " + names.base() + " (shard, message_id, topic)"
                    + " select 0, gen_random_uuid(), 't' from generate_series(1, 5)");
            c.setAutoCommit(false);
            execute(c, "insert into " + names.base()
                    + " (shard, message_id, topic) values (0, gen_random_uuid(), 't')");
            c.rollback();
            c.setAutoCommit(true);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(ticks.latest(c).maxId()).as("rolled-back ids count too").isEqualTo(6);
        }
    }

    @Test
    void aTickRecordsTheShardsThatGainedRowsSinceThePreviousTick() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            insertRow(c, 3); // autocommit: three transactions
            insertRow(c, 7);
            insertRow(c, 3);

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).containsExactly(3, 7);

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).as("no row since the previous tick").isEmpty();
        }
    }

    @Test
    void dirtySetIsABitmap() throws Exception {
        var sixteen = new Ticks(names, 16);
        try (var c = TestDb.connect()) {
            assertThat(sixteen.make(c, Duration.ZERO)).isTrue();
            insertRow(c, 3);
            insertRow(c, 7);

            assertThat(sixteen.make(c, Duration.ZERO)).isTrue();

            assertThat(dirty(c)).as("bit s (leftmost 0) set for shard s").isEqualTo("0001000100000000");
        }
    }

    @Test
    void clusteredProbeFindsLongAndShortTransactions() throws Exception {
        try (var longTx = TestDb.connect(); var first = TestDb.connect(); var last = TestDb.connect();
                var c = TestDb.connect()) {
            longTx.setAutoCommit(false);
            insertRow(longTx, 1);
            long longXid = xid(longTx);
            execute(c, "do $$ begin for i in 1..100000 loop perform pg_current_xact_id(); commit; end loop; end $$");
            first.setAutoCommit(false);
            insertRow(first, 2);
            insertRow(c, 4); // committed before P, between the two short ones: inside their cluster's range
            last.setAutoCommit(false);
            insertRow(last, 5);
            xid(c); // a later xid completes: P's xmax passes the three
            assertThat(ticks.make(c, Duration.ZERO)).isTrue(); // P
            long committed = queryLong(c, "select xid::text::bigint from " + names.base() + " where shard = 4");
            assertThat(ticks.latest(c).snapshot().xip()).as("in flight at P")
                    .contains(longXid, xid(first), xid(last)).doesNotContain(committed);
            assertThat(xid(first)).as("a gap far above the cluster gap").isGreaterThan(longXid + 100_000);
            assertThat(committed).isBetween(xid(first), xid(last));
            assertThat(xid(last) - xid(first)).as("one cluster").isLessThanOrEqualTo(1024);
            for (var tx : List.of(longTx, first, last)) {
                tx.commit();
            }

            assertThat(ticks.make(c, Duration.ZERO)).isTrue(); // C

            assertThat(shards(c)).as("shard 4's row is visible in P").containsExactly(1, 2, 5);
        }
    }

    @Test
    void aTransactionRunningAtThePreviousTickCountsInTheTickThatSeesItCommitted() throws Exception {
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            writer.setAutoCommit(false);
            insertRow(writer, 5);
            long xid = xid(writer);
            xid(c); // a later xid completes: the writer is below xmax
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(ticks.latest(c).snapshot().xip()).as("in flight at P").contains(xid);
            assertThat(shards(c)).doesNotContain(5);

            writer.commit();
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(shards(c)).containsExactly(5); // P.xip minus C.xip
        }
    }

    @Test
    void aTransactionRunningAtATickCountsInTheTickWhereItBecomesVisible() throws Exception {
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            writer.setAutoCommit(false);
            insertRow(writer, 6); // xid at or above the next tick's xmax, or in its xip
            insertRow(c, 1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).containsExactly(1);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).as("still running").isEmpty();

            writer.commit();
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(shards(c)).containsExactly(6);
        }
    }

    @Test
    void aRolledBackTransactionMarksAtMostItsOwnShard() throws Exception {
        try (var writer = TestDb.connect(); var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            writer.setAutoCommit(false);
            insertRow(writer, 2);
            writer.rollback();

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).as("a superset is allowed, nothing beyond its shard").isSubsetOf(2);

            insertRow(c, 2);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).containsExactly(2);
        }
    }

    @Test
    void aTickWithoutAPreviousOneStoresNoShardSetMeaningAll() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(shards(c)).as("the bootstrap tick").isNull();
            insertRow(c, 4);
            execute(c, "delete from " + names.tick());

            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(shards(c)).isNull();
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).isEmpty();
        }
    }

    @Test
    void aTickProbesEachShardOnceHoweverManyRowsItCovers() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            execute(c, "insert into " + names.base() + " (shard, message_id, topic, payload)"
                    + " select g % " + SHARDS + ", gen_random_uuid(), 't', repeat('x', 100)::bytea"
                    + " from generate_series(1, 20000) g");

            // the plan Ticks.make runs: generic (see Ticks.lock), where an exists per shard may become one hashed pass
            // over all 20 000 rows
            assertThat(probeBuffers(c, ticks)).as("buffers of a tick over 20 000 new rows on %d shards", SHARDS)
                    .isLessThan(200);
        }
    }

    /**
     * A generic plan takes {@code generate_series} over a parameter for 1000 rows, whatever the shard count: on a big
     * partition that lifts the plan's estimate over {@code jit_above_cost}, and JIT then compiles the probe at every
     * execution, under the tick lock.
     */
    @Test
    void theProbesGenericPlanKnowsTheShardCount() throws Exception {
        try (var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();

            assertThat(String.join("\n", probePlan(c, ticks)))
                    .containsPattern("generate_series g +\\(cost=\\S+ rows=" + SHARDS + " ");
        }
    }

    @Test
    void aClusterSplitsWhereManyRowsCommittedBetweenItsXids() throws Exception {
        try (var first = TestDb.connect(); var last = TestDb.connect(); var c = TestDb.connect()) {
            assertThat(ticks.make(c, Duration.ZERO)).isTrue(); // the tick before P: the rows per xid up to P
            first.setAutoCommit(false);
            xid(first); // ends with no row
            // committed before P between the two: inside their cluster's range, 2 500 rows per shard
            execute(c, "insert into " + names.base() + " (shard, message_id, topic) select g % " + SHARDS
                    + ", gen_random_uuid(), 't' from generate_series(1, 20000) g");
            last.setAutoCommit(false);
            insertRow(last, 2);
            xid(c); // a later xid completes: P's xmax passes the two
            assertThat(ticks.make(c, Duration.ZERO)).isTrue(); // P
            assertThat(xid(last) - xid(first)).as("within the cluster gap").isLessThanOrEqualTo(Ticks.CLUSTER_GAP);
            first.commit();
            last.commit();

            assertThat(probeBuffers(c, ticks))
                    .as("buffers of a tick whose ended xids have 20 000 rows between them").isLessThan(200);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue(); // C
            assertThat(shards(c)).containsExactly(2);
        }
    }

    @Test
    void aTickWhoseEstimatedRangeRowsExceedTheBudgetMarksEveryShardUnprobed() throws Exception {
        var wide = new Ticks(names, 1024); // where a gap of a few rows per xid stays inside a cluster
        var ended = new ArrayList<Connection>();
        try (var c = TestDb.connect()) {
            assertThat(wide.make(c, Duration.ZERO)).isTrue(); // the tick before P: the rows per xid up to P
            for (int i = 0; i < 16; i++) {
                var tx = TestDb.connect();
                ended.add(tx);
                tx.setAutoCommit(false);
                xid(tx); // ends with no row
                if (i < 15) { // committed before P between two ended xids: about 2 000 rows per xid, no split
                    execute(c, "insert into " + names.base() + " (shard, message_id, topic) select g % " + SHARDS
                            + ", gen_random_uuid(), 't' from generate_series(1, 4400) g");
                }
            }
            xid(c); // a later xid completes: P's xmax passes the 16
            assertThat(wide.make(c, Duration.ZERO)).isTrue(); // P
            for (var tx : ended) {
                tx.commit();
            }

            assertThat(probeBuffers(c, wide)).as("buffers of a probe that scans neither branch")
                    .isLessThan(100);
            assertThat(wide.make(c, Duration.ZERO)).isTrue(); // C
            assertThat(dirty(c)).as("about 30 000 rows estimated inside the cluster's range: every shard").isNull();
        } finally {
            for (var tx : ended) {
                tx.close();
            }
        }
    }

    @Test
    void withTheRowsPerXidUnknownTheProbeHasNoBudget() throws Exception {
        var wide = new Ticks(names, 1024);
        var ended = new ArrayList<Connection>();
        try (var c = TestDb.connect()) {
            for (int i = 0; i < 6; i++) { // 6 clusters: 6 × 1024 range probes, over the budget if it applied
                var tx = TestDb.connect();
                ended.add(tx);
                tx.setAutoCommit(false);
                xid(tx); // ends with no row
                execute(c, "do $$ begin for i in 1..1100 loop perform pg_current_xact_id(); commit; end loop; end $$");
            }
            xid(c); // a later xid completes: P's xmax passes the 6
            assertThat(wide.make(c, Duration.ZERO)).isTrue(); // P
            wide.deleteBelow(c, wide.latest(c).id()); // P alone: no tick before it, so no rows per xid
            for (var tx : ended) {
                tx.commit();
            }

            assertThat(wide.make(c, Duration.ZERO)).isTrue(); // C
            assertThat(dirty(c)).as("probed: the budget needs the rows per xid").isNotNull().doesNotContain("1");
        } finally {
            for (var tx : ended) {
                tx.close();
            }
        }
    }

    @Test
    void aTickTakesOneTransactionIdItsProbeNone() throws Exception {
        try (var c = TestDb.connect()) {
            insertRow(c, 1);
            for (int i = 0; i < 10; i++) {
                assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            }
            // gaps between the xids of consecutive tick rows: a stray xid elsewhere (autoanalyze) widens one gap only
            long gap = queryLong(c, "select min(gap) from (select xmin::text::bigint - lag(xmin::text::bigint)"
                    + " over (order by id) gap from " + names.tick() + " order by id desc limit 10) t");

            assertThat(gap).as("xids per tick, its probe included").isEqualTo(1);
        }
    }

    @Test
    void aLockedPartitionCostsATickItsShardSetNotTheTick() throws Exception {
        try (var c = TestDb.connect(); var other = TestDb.connect()) {
            for (int i = 0; i < 6; i++) { // a cached generic plan, as on a pooled connection
                assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            }
            var premade = queryString(c, "select inhrelid::regclass::text from pg_inherits i join pg_class k"
                    + " on k.oid = i.inhrelid where inhparent = '" + names.base() + "'::regclass and k.relname <> '"
                    + names.unqualified(names.tail()) + "' order by k.oid desc limit 1"); // empty, far above the xid
            for (var locked : List.of(premade, names.tail())) {
                other.setAutoCommit(false);
                execute(other, "lock table " + locked + " in access exclusive mode"); // e.g. an ALTER TABLE by hand
                long started = System.nanoTime();

                assertThat(ticks.make(c, Duration.ZERO)).as("a tick while %s is locked", locked).isTrue();

                assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(500));
                assertThat(shards(c)).as("no shard set: every shard may have rows").isNull();
                assertSessionRestored(c);
                other.rollback();
            }

            insertRow(c, 6);
            assertThat(ticks.make(c, Duration.ZERO)).isTrue();
            assertThat(shards(c)).as("probed again once the lock is gone").containsExactly(6);
        }
    }

    @Test
    void aProbeThatFailsFallsBackToATickWithoutAShardSetAndWarnsOnce() throws Exception {
        var login = probelessLogin();
        try (var err = new CapturedStderr(); var asRole = TestDb.connect(login); var c = TestDb.connect()) {
            var roleTicks = new Ticks(names, SHARDS);
            insertRow(c, 3);

            assertThat(roleTicks.make(asRole, Duration.ZERO)).isTrue();
            assertThat(shards(c)).isNull();
            assertThat(roleTicks.make(asRole, Duration.ZERO)).isTrue();

            assertThat(shards(c)).isNull();
            assertSessionRestored(asRole);
            assertThat(err.text()).contains("42501").containsOnlyOnce("WARN " + Ticks.class.getName());
        }
    }

    @Test
    void aSnapshotBelowTheNewestTicksXmaxMakesNoTickAndRecordsTheHalt() throws Exception {
        var login = probelessLogin(); // a role whose probe fails, so it inserts without one
        try (var asRole = TestDb.connect(login); var c = TestDb.connect()) {
            // the newest tick from a history 10 000 xids ahead, as a logical cutover onto a younger cluster leaves it
            long shifted = queryLong(c, "insert into " + names.tick() + " (snapshot, max_id) select ("
                    + "(pg_snapshot_xmin(s)::text::bigint + 10000) || ':' || (pg_snapshot_xmax(s)::text::bigint"
                    + " + 10000) || ':' || array_to_string(array(select x::text::bigint + 10000 from"
                    + " pg_snapshot_xip(s) x), ','))::pg_snapshot, 0 from (select pg_current_snapshot() s) q"
                    + " returning pg_snapshot_xmax(snapshot)::text::bigint");
            long ticksBefore = queryLong(c, "select count(*) from " + names.tick());

            assertThat(new Ticks(names, SHARDS).tick(asRole, Duration.ZERO)).as("without a probe: no tick, no halt")
                    .isEqualTo(Ticks.Made.SKIPPED);
            assertThat(queryLong(c, "select count(*) from " + names.halt())).isZero();

            assertThat(ticks.tick(c, Duration.ZERO)).isEqualTo(Ticks.Made.REGRESSED);
            assertThat(queryLong(c, "select count(*) from " + names.tick())).isEqualTo(ticksBefore);
            assertThat(queryLong(c, "select max_tick_xmax::text::bigint from " + names.halt())).isEqualTo(shifted);
            assertThat(queryString(c, "select recovery_failed from " + names.halt())).isNull();
            assertSessionRestored(c);
        }
    }

    /**
     * The shared buffers the probe of a tick made now touches, in the plan {@link Ticks#make} runs (generic, see
     * {@code Ticks.lock}); rolled back, so no tick is made.
     */
    private static long probeBuffers(Connection c, Ticks ticks) throws Exception {
        long buffers = 0;
        for (var line : probePlan(c, ticks)) {
            var m = Pattern.compile("(hit|read)=(\\d+)").matcher(line);
            while (m.find()) {
                buffers += Long.parseLong(m.group(2));
            }
            if (buffers != 0) { // the first Buffers line: the whole statement's
                break;
            }
        }
        return buffers;
    }

    /** The lines of {@code explain (analyze, buffers)} of the probe, on the generic plan {@link Ticks#make} runs. */
    private static List<String> probePlan(Connection c, Ticks ticks) throws Exception {
        c.setAutoCommit(false);
        execute(c, "set local plan_cache_mode = force_generic_plan");
        execute(c, "prepare tick_probe(bigint) as " + ticks.probe(c).replaceFirst("\\?", "\\$1"));
        var plan = new ArrayList<String>();
        try (var st = c.createStatement();
                var rs = st.executeQuery("explain (analyze, buffers) execute tick_probe(0)")) {
            while (rs.next()) {
                plan.add(rs.getString(1));
            }
        }
        c.rollback();
        execute(c, "deallocate tick_probe");
        c.setAutoCommit(true);
        return plan;
    }

    /** One row of {@code shard} in the caller's transaction (its own one in autocommit). */
    private void insertRow(Connection c, int shard) throws Exception {
        execute(c, "insert into " + names.base() + " (shard, message_id, topic) values (" + shard
                + ", gen_random_uuid(), 't')");
    }

    /** The shards whose bit is set in the newest tick's {@code dirty}, null when it has none. */
    @Nullable
    private List<Integer> shards(Connection c) throws Exception {
        var dirty = dirty(c);
        return dirty == null ? null
                : IntStream.range(0, dirty.length()).filter(s -> dirty.charAt(s) == '1').boxed().toList();
    }

    /** The newest tick's {@code dirty} as text, leftmost bit shard 0. */
    @Nullable
    private String dirty(Connection c) throws Exception {
        return queryString(c, "select dirty::text from " + names.tick() + " order by id desc limit 1");
    }

    /** A new role's login that may make ticks but not read the outbox: its probe fails (42501). */
    private Properties probelessLogin() throws Exception {
        var role = TestDb.newLoginRole();
        try (var c = TestDb.connect()) {
            execute(c, "grant select, insert on " + names.tick() + " to " + role);
            execute(c, "grant select on sequence "
                    + queryString(c, "select pg_get_serial_sequence('" + names.base() + "', 'id')") + " to " + role);
        }
        return TestDb.login(role);
    }

    private static long xid(Connection c) throws Exception {
        return queryLong(c, "select pg_current_xact_id()::text::bigint");
    }

    private static void awaitLockWait(Connection observer, long pid) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        while (queryLong(observer, "select count(*) from pg_locks where not granted and pid = " + pid) == 0) {
            assertThat(System.nanoTime()).as("pid %d waits for a lock", pid).isLessThan(deadline);
            Thread.sleep(1);
        }
    }

    private static void assertSessionRestored(Connection c) throws Exception {
        assertThat(c.getAutoCommit()).isTrue();
        assertThat(queryString(c, "show lock_timeout")).isEqualTo("0");
        assertThat(queryString(c, "show idle_in_transaction_session_timeout")).isEqualTo("0");
        assertThat(queryString(c, "show plan_cache_mode")).isEqualTo("auto");
    }
}
