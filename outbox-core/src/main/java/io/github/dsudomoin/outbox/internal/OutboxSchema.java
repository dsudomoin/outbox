package io.github.dsudomoin.outbox.internal;

import static io.github.dsudomoin.outbox.internal.Sql.queryLong;

import io.github.dsudomoin.outbox.ConnectionSource;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxTransactions;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Idempotent startup bootstrap: tables, the hot-table index, premade xid partitions with the tail, one initial tick
 * and one cursor per shard, all under {@code LOCK_SCHEMA}, which a {@link LockHolder} on a second connection holds
 * ({@code pg_advisory_xact_lock}); like every lock namespace, {@code internalNs} comes from the table
 * the configured name resolves to ({@link TableNames#resolve}), which {@link #names()} hands to the other components.
 * The shard count ({@link #shards}): the cursors' count when there are cursors, else {@link Tuning#shards()}. Fails
 * fast with {@link IllegalStateException} when an identity sequence of the outbox, tick or cursor table has
 * {@code CACHE > 1}. An xid regression (the counter below a tick's xmax) never fails it: it is recorded in
 * {@code <base>_halt} and, in {@link Tuning#xidRegression()} {@code auto} mode with at most
 * {@link Tuning#recoveryMaxRows()} rows to re-send, recovered before any relay starts ({@link XidRecovery}, under
 * {@code LOCK_MAINTENANCE}, taken after {@code LOCK_SCHEMA}); otherwise, or while another session holds that lock,
 * startup goes on halted (the relay delivers nothing, writes go on) with an ERROR naming the admin call that runs the
 * recovery. The recovery runs under its own timeouts (one deadline of {@code outbox.recovery.timeout}, 10 min by
 * default), and other instances' startups wait for it behind the schema lock, at most a minute. The schema lock and
 * every bootstrap DDL statement wait at most 5 s for a lock ({@code lock_timeout}), and every DDL statement runs under
 * a 1 min {@code statement_timeout} (replacing a shorter role default), each in a transaction of its own with both
 * {@code SET LOCAL}: nothing is set for the session, so the bootstrap runs behind a transaction-pooling proxy
 * (pgbouncer, Odyssey) too. A DDL statement that needs longer, such as an index built anew on a populated table, fails
 * startup with SQLState 57014 and is left to run by hand; the dead table's {@code <dead>_due}, new with the dead-letter
 * hook, is built here only while that table has no row, otherwise (an INFO) by {@link Maintenance}, concurrently. The
 * schema lock is retried for a minute, then startup fails. On existing tables the bootstrap takes no lock that writers
 * wait behind (index creation is skipped when the index exists, partition extension is optional and left to maintenance
 * when it times out). A tick table made before {@code max_id}, {@code dirty} or {@code shards}, a cursor table made
 * before {@code parked_due_at}, a retry table made before {@code requeues} or {@code waiting_since} (and its {@code
 * <retry>_waiting} index), an instance table made before {@code relay}, {@code contracts} or {@code routes} and a dead
 * table made before the dead-letter hook's columns get each column once; such an ALTER waits for running relay
 * transactions and fails startup on timeout. The tick table keeps {@code shards}, the shard sets {@code dirty}
 * replaces, for the previous release, which writes and reads it beside or after this one; this version leaves it null
 * (every shard). Ticks without {@code dirty} (null) count as holding rows of every shard; dead letters older than
 * {@code status} are kept and the hook sees new ones only: one ALTER adds the hook's columns, and their check is not
 * validated, so it scans no row of the dead table under that ALTER's lock. With {@code outbox.schema.bootstrap = false}
 * it creates and alters no base object: {@link SchemaCheck} checks them first, failing startup with every problem, and
 * the rest (partitions, the first tick, cursors, the maintenance and instance rows, the checks above) runs as with it.
 */
public final class OutboxSchema implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OutboxSchema.class);
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(5);
    /** Bounds every bootstrap statement, and replaces a shorter role default that would cut its lock waits short. */
    private static final Duration STATEMENT_TIMEOUT = Duration.ofMinutes(1);
    /**
     * How long the schema lock's holder may stay silent before Postgres ends it: short of the minute other bootstraps
     * wait for the schema lock, so a start the network cut off frees its locks in time for them. Its keepalive speaks
     * every {@link LockHolder#KEEPALIVE}.
     */
    private static final Duration HOLDER_IDLE = Duration.ofSeconds(45);
    /** {@code LOCK_TIMEOUT} each: a minute for other instances' bootstraps, which take milliseconds each. */
    private static final int SCHEMA_LOCK_ATTEMPTS = 12;
    private static final String LOCK_NOT_AVAILABLE = "55P03";
    private static final String QUERY_CANCELED = "57014";
    private static final String INSUFFICIENT_PRIVILEGE = "42501";
    private static final String DUPLICATE_TABLE = "42P07";
    /** Bound of {@code deadLetters.retention}: 10 years, an age maintenance compares. */
    private static final Duration MAX_DEAD_LETTER_AGE = Duration.ofDays(3650);
    /**
     * Bound of the timeouts sent to Postgres as millisecond settings ({@code lock_timeout}, {@code statement_timeout},
     * {@code idle_in_transaction_session_timeout}), which take at most 2^31-1 ms, about 24.8 days: the rest covers the
     * margins added to them (batchTimeout + 65 s in the swap's quiesce, swapLockTimeout + 60 s in DDL).
     */
    private static final Duration MAX_PG_TIMEOUT = Duration.ofDays(24);
    /** Heartbeat rows (this one's too) stay live for this many of the writer's {@code maintenanceInterval}. */
    static final int LIVE_INTERVALS = 3;

    private final OutboxTransactions db;
    /** Where the bootstrap runs: {@link #db}, or the owner's connections ({@link OwnerTransactions}). */
    private final OutboxTransactions ddl;
    private final OutboxConfig config;
    private final TableNames spelled;
    private final int schemaLockAttempts;
    private final String instanceId = Heartbeat.newId();
    /** {@link #asciiText(Connection, TableNames)} as the bootstrap read it. */
    private boolean asciiText;
    private final ContractGuard contracts;
    /** The heartbeat's own connections, which fall back on {@link #db}; null without a source of its own. */
    @Nullable
    private final HeartbeatConnections heartbeat;
    private final SocketBound socket;
    @Nullable
    private volatile TableNames names;
    /** The names the bootstrap wrote this instance's row under, for {@link #close()} after a failed start. */
    @Nullable
    private volatile TableNames written;
    @Nullable
    private volatile OutboxConfig resolved;

    /**
     * Validates the whole outbox config: every other outbox component starts after this one, so a bad value fails
     * startup before any DDL or relay thread.
     */
    public OutboxSchema(OutboxTransactions db, OutboxConfig config) {
        this(db, db, config);
    }

    /** {@code ddl}: the transactions whose new connections run the bootstrap, the owner's ones when it has its own. */
    public OutboxSchema(OutboxTransactions db, OutboxTransactions ddl, OutboxConfig config) {
        this(db, ddl, config, Map.of(), Set.of());
    }

    /**
     * {@code contracts}: the contract version each typed route of the instance reads, by topic; {@code routes}: every
     * topic a route of the instance claims, typed or plain. Its row reports both, the routes sorted.
     */
    public OutboxSchema(OutboxTransactions db, OutboxTransactions ddl, OutboxConfig config,
            Map<String, Integer> contracts, Set<String> routes) {
        this(db, ddl, config, contracts, routes, null);
    }

    /**
     * {@code heartbeat}: connections for the heartbeat alone, its beats and its row's delete, null for those of
     * {@code db}. One that reaches another database than {@code db}'s fails {@link #start()} when seen there; seen only
     * later, as when it could not connect at start, it is reported by an ERROR, and the heartbeat keeps {@code db}'s
     * connections for a few {@code maintenanceInterval}s, then checks it again.
     */
    public OutboxSchema(OutboxTransactions db, OutboxTransactions ddl, OutboxConfig config,
            Map<String, Integer> contracts, Set<String> routes, @Nullable ConnectionSource heartbeat) {
        this(db, ddl, config, SCHEMA_LOCK_ATTEMPTS, contracts, routes, heartbeat);
    }

    OutboxSchema(OutboxTransactions db, OutboxConfig config, int schemaLockAttempts) {
        this(db, db, config, schemaLockAttempts, Map.of(), Set.of(), null);
    }

    private OutboxSchema(OutboxTransactions db, OutboxTransactions ddl, OutboxConfig config, int schemaLockAttempts,
            Map<String, Integer> contracts, Set<String> routes, @Nullable ConnectionSource heartbeat) {
        validate(config);
        this.db = db;
        this.ddl = ddl;
        this.config = config;
        this.spelled = TableNames.of(config.table());
        this.schemaLockAttempts = schemaLockAttempts;
        var tuning = Tuning.of(config);
        this.contracts = new ContractGuard(contracts, routes, tuning.maintenanceInterval());
        this.heartbeat = heartbeat == null ? null
                : new HeartbeatConnections(db, heartbeat, Heartbeat.socketBound(config), tuning.maintenanceInterval());
        this.socket = new SocketBound(tuning.networkTimeout());
    }

    /**
     * The one check of the outbox config ({@link InboxPartitions} checks the inbox keys); {@link Tuning} values are not
     * checked, except where a check combines one with a key. The other components that take the config run it too
     * ({@link io.github.dsudomoin.outbox.OutboxRelay}, {@link ShardProcessor}), so built on their own they reject the
     * same values with the same messages.
     */
    public static void validate(OutboxConfig config) {
        var relay = config.relay();
        var retry = relay.retry();
        var requeue = relay.requeue();
        var deadLetters = config.deadLetters();
        require(relay.workers() >= 1, "outbox.relay.workers must be at least 1: " + relay.workers());
        require(relay.batchSize() >= 1, "outbox.relay.batchSize must be at least 1: " + relay.batchSize());
        require(relay.batchBytes() >= 0, "outbox.relay.batchBytes must not be negative: " + relay.batchBytes());
        require(relay.batchTimeout().compareTo(Duration.ZERO) > 0,
                "outbox.relay.batchTimeout must be positive: " + relay.batchTimeout());
        requireAtMost24d("outbox.relay.batchTimeout", relay.batchTimeout());
        require(!relay.shutdownTimeout().isNegative(),
                "outbox.relay.shutdownTimeout must not be negative: " + relay.shutdownTimeout());
        require(retry.initialDelay().compareTo(Duration.ZERO) > 0,
                "outbox.relay.retry.initialDelay must be positive: " + retry.initialDelay());
        require(retry.maxDelay().compareTo(retry.initialDelay()) >= 0,
                "outbox.relay.retry.maxDelay must be at least outbox.relay.retry.initialDelay: " + retry.maxDelay());
        require(retry.maxAttempts() >= 0,
                "outbox.relay.retry.maxAttempts must not be negative: " + retry.maxAttempts());
        require(requeue.maxRequeues() >= 0,
                "outbox.relay.requeue.maxRequeues must not be negative: " + requeue.maxRequeues());
        var tail = config.partitions().tailConversion();
        XidPartitions.Window.parse(tail.window()); // its message names the key
        try {
            java.time.ZoneId.of(tail.zone());
        } catch (java.time.DateTimeException e) {
            throw new IllegalArgumentException("outbox.partitions.tailConversion.zone must be a time zone id such as"
                    + " Europe/Moscow or UTC: " + tail.zone(), e);
        }
        require(tail.maxStall().toMillis() >= 1,
                "outbox.partitions.tailConversion.maxStall must be at least 1ms: " + tail.maxStall());
        requireAtMost24d("outbox.partitions.tailConversion.maxStall", tail.maxStall());
        // a queued swap of an oversized tail runs under statement_timeout = swapLockTimeout + maxStall
        var swapWait = Tuning.of(config).swapLockTimeout();
        if (!swapWait.isZero()) {
            requireAtMost24d("outbox.partitions.tailConversion.maxStall plus the swap's queued wait of " + swapWait,
                    swapWait.plus(tail.maxStall()));
        }
        require(!deadLetters.retention().isNegative(),
                "outbox.deadLetters.retention must not be negative: " + deadLetters.retention());
        require(deadLetters.retention().compareTo(MAX_DEAD_LETTER_AGE) <= 0,
                "outbox.deadLetters.retention must be at most 3650d: " + deadLetters.retention());
        var recovery = config.recovery();
        require(recovery.timeout().toMillis() > 0, "outbox.recovery.timeout must be positive: " + recovery.timeout());
        requireAtMost24d("outbox.recovery.timeout", recovery.timeout());
    }

    /** The shard count a component writes or reads with, as the cursors' {@code smallint} shard takes it. */
    static void validateShards(int shards) {
        require(shards >= 1 && shards <= Short.MAX_VALUE,
                "outbox shard count must be within 1.." + Short.MAX_VALUE + ": " + shards);
    }

    private static void requireAtMost24d(String key, Duration value) {
        require(value.compareTo(MAX_PG_TIMEOUT) <= 0, key + " must be at most 24d: " + value);
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * The table names with the lock namespaces of the table {@link #start()} resolved, which every component locks
     * with: an instance configured {@code outbox} and one configured {@code public.outbox} share all locks.
     */
    public TableNames names() {
        var n = names;
        if (n == null) {
            throw new IllegalStateException("OutboxSchema.start() has not run: the lock namespaces of "
                    + spelled.base() + " come from the table it resolves to");
        }
        return n;
    }

    /**
     * The config every other component runs with: this one with the shard count {@link #start()} resolved
     * ({@link #shards}), never null.
     */
    public OutboxConfig config() {
        var r = resolved;
        if (r == null) {
            throw new IllegalStateException("OutboxSchema.start() has not run: the shard count of " + spelled.base()
                    + " comes from its cursors");
        }
        return r;
    }

    /**
     * This instance's id in {@code <base>_instance}, {@code <host>/<pid>/<8 hex>}: the bootstrap writes its row, the
     * relay's {@link Heartbeat} keeps it fresh.
     */
    public String instanceId() {
        return instanceId;
    }

    /**
     * This instance's contracts and routes, and the live relay instances' reports of theirs as this instance last read
     * them, which its sends check.
     */
    ContractGuard contractGuard() {
        return contracts;
    }

    /** The heartbeat's own connections, which fall back on the application's; null without a source of its own. */
    @Nullable
    ConnectionSource heartbeatConnections() {
        return heartbeat;
    }

    /**
     * {@code config} with the shard count resolved at bootstrap ({@link #shards(OutboxConfig)}); its {@link Tuning} is
     * {@code config}'s ({@link Tuning#of}); {@code asciiText}: see {@link #asciiText(OutboxConfig)}.
     */
    record Resolved(OutboxConfig config, int shards, boolean asciiText) implements OutboxConfig {

        @Override
        public String table() {
            return config.table();
        }

        @Override
        public RelayConfig relay() {
            return config.relay();
        }

        @Override
        public PartitionsConfig partitions() {
            return config.partitions();
        }

        @Override
        public InboxConfig inbox() {
            return config.inbox();
        }

        @Override
        public DeadLettersConfig deadLetters() {
            return config.deadLetters();
        }

        @Override
        public RecoveryConfig recovery() {
            return config.recovery();
        }

        @Override
        public SchemaConfig schema() {
            return config.schema();
        }
    }

    /**
     * The checks first, on a connection given back before the schema lock is waited for; then, the lock held by a
     * {@link LockHolder}, the bootstrap on a connection of its own: none is held while waiting for another, and no
     * connection idles through the wait.
     */
    public void start() throws SQLException {
        TableNames resolved;
        try (var c = ddl.newConnection(); var bound = socket.on(c)) {
            if (ddl != db) { // before any DDL
                try (var app = db.newConnection(); var appBound = socket.on(app)) {
                    SchemaCheck.requireSameDatabase(app, c, spelled.base());
                    SchemaCheck.warnIfAppCanCreateWhereOwnerResolves(app, c, spelled.base());
                    PreparedStatements.requireKept(app, "outbox start of " + spelled.base());
                }
            }
            resolved = autocommit(c, () -> {
                PreparedStatements.requireKept(c, "outbox bootstrap of " + spelled.base());
                PartitionDdl.warnIfDetachDropUnfixed(c, "outbox bootstrap of " + spelled.base());
                // where the table is or will be created, so every spelling of it takes the same schema lock
                var r = spelled.resolve(c);
                if (heartbeat != null) { // before any DDL, and before the bootstrap writes the row
                    heartbeat.start(c, r.base());
                }
                return r;
            });
        }
        try (var holder = LockHolder.open(ddl, socket, HOLDER_IDLE)) {
            lockSchema(holder, resolved);
            try (var c = ddl.newConnection(); var bound = socket.on(c); var scope = holder.bind(c)) {
                // a statement runs for as long as its timeout: silence only past that says the network dropped it
                autocommit(c, () -> SocketBound.atLeast(c, STATEMENT_TIMEOUT, () -> {
                    bootstrap(c, resolved, holder);
                    names = spelled.resolve(c); // the table exists now: checks that the name finds it there
                    return null;
                }));
            }
        }
    }

    /** {@code body} on {@code c} in autocommit, {@code c}'s own mode given back after it whatever happens. */
    private static <T> T autocommit(Connection c, Sql.SqlFunction0<T> body) throws SQLException {
        boolean autoCommit = c.getAutoCommit();
        c.setAutoCommit(true);
        T result;
        try {
            result = body.apply();
        } catch (Throwable e) {
            try {
                c.setAutoCommit(autoCommit);
            } catch (SQLException cleanup) { // the session died
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        c.setAutoCommit(autoCommit);
        return result;
    }

    /**
     * Deletes this instance's row, within {@link Heartbeat#STOP_BUDGET}, or leaves it to expire: for a schema without a
     * relay, whose {@link Heartbeat} does it otherwise, and for a {@link #start()} that failed after the bootstrap
     * wrote it.
     */
    @Override
    public void close() {
        var n = names != null ? names : written;
        if (n != null) {
            Heartbeat.delete(heartbeat != null ? heartbeat : db, n, instanceId, Heartbeat.STOP_BUDGET);
        }
    }

    private void bootstrap(Connection c, TableNames n, LockHolder holder) throws SQLException {
        try (var st = c.createStatement()) {
            if (config.schema().bootstrap()) {
                createBaseObjects(c, n);
            } else { // the application's migrations own them: only checked, and nothing created when one is missing
                try (var app = db.newConnection(); var appBound = socket.on(app)) {
                    SchemaCheck.run(app, c, SchemaObjects.outbox(n), spelled.base(), "OutboxSql.outbox");
                }
            }
            int present = bounded(c, () -> {
                // when a maintenance run last completed every step, on any instance; until the first one, bootstrap
                // time
                st.execute("insert into " + n.maintenance() + " (succeeded_at) values (now())"
                        + " on conflict (id) do nothing");
                checkSequenceCache(c, n);
                raiseRewoundSequences(c, n); // before this instance makes a tick or a cursor row
                asciiText = asciiText(c, n);
                return (int) queryLong(st, Cursors.shardCount(n));
            });
            int shards = present == 0 ? Tuning.of(config).shards() : present;
            var r = new Resolved(config, shards, asciiText);
            checkXidCounter(c, n, r, holder);

            ensurePartitions(c, n, holder);

            bounded(c, () -> {
                st.execute("insert into " + n.tick() + " (snapshot, max_id) select pg_current_snapshot(), "
                        + Ticks.maxIdQuery(c, n) + " where not exists (select 1 from " + n.tick() + ")");
                // not after a recovery, which makes them
                try (var ps = c.prepareStatement(insertCursors(n) + " where not exists (select 1 from " + n.cursor()
                        + ")")) {
                    ps.setInt(1, shards);
                    ps.executeUpdate();
                }
                try (var ps = c.prepareStatement(Heartbeat.upsert(n))) {
                    Heartbeat.bind(ps, 0, instanceId, shards, false, config.relay().enabled(), contracts.contracts(),
                            contracts.routes(), Tuning.of(config).maintenanceInterval());
                    ps.executeUpdate();
                }
                return null;
            });
            written = n; // committed already: a failure from here on leaves it for close() to delete
            contracts.refresh(c, n); // this row too: a send right after start is guarded
            resolved = r;
        }
    }

    /** Creates the missing base objects ({@link SchemaObjects}) and upgrades the ones earlier versions made. */
    private void createBaseObjects(Connection c, TableNames n) throws SQLException {
        // the dead-letter hook's columns, on a fresh table too, in one ALTER while any is missing (due_at: the next
        // hook call of a pending letter, the redrive of a scheduled one); letters of a table made before them become
        // kept, so the status check, added with its column, skips them (not valid). Read before the tables are made:
        // a fresh table has the columns, and still takes the check from the ALTER
        boolean noStatus = !columnExists(c, n.dead(), "status");
        boolean missing = noStatus;
        for (var column : List.of("due_at", "handler_attempts", "last_handler_error", "redrives")) {
            missing |= !columnExists(c, n.dead(), column);
        }
        var objects = SchemaObjects.outbox(n);
        for (var table : objects.tables()) {
            ddl(c, table.createSql(true));
        }
        addColumn(c, n.tick(), "max_id", "bigint"); // a tick table made before max_id: a metadata-only change
        addColumn(c, n.tick(), "dirty", "bit varying"); // made before dirty: likewise; its ticks stay null (all)
        // the shard sets dirty replaces, kept for the previous release beside or after this one: it inserts them in
        // every tick and reads them (null: every shard, as this version leaves them). Made again where an earlier
        // build of this version dropped it
        addColumn(c, n.tick(), "shards", "smallint[]");
        // made before requeue: metadata-only, rows get null
        addColumn(c, n.cursor(), "parked_due_at", "timestamptz");
        addColumn(c, n.retry(), "requeues", "int not null default 0"); // made before requeue: a constant default
        addColumn(c, n.retry(), "waiting_since", "timestamptz"); // made before WAIT: rows get null, not waiting
        if (missing) {
            ddl(c, "alter table " + n.dead() + " add column if not exists status text not null default 'kept',"
                    + " add column if not exists due_at timestamptz,"
                    + " add column if not exists handler_attempts int not null default 0,"
                    + " add column if not exists last_handler_error text,"
                    + " add column if not exists redrives int not null default 0"
                    + (noStatus ? ", add constraint " + n.unqualified(n.dead()) + "_status"
                            + " check (status in ('pending', 'kept', 'scheduled')) not valid" : ""));
        }
        addColumn(c, n.halt(), "recovery_failed", "text"); // the previous release's table
        // XidPartitions: when any instance first saw the hot tail over maxTailScan, null while none is
        addColumn(c, n.maintenance(), "tail_oversized_since", "timestamptz"); // the previous release's table
        addColumn(c, n.instance(), "expires_at", "timestamptz"); // made by an earlier build of this version
        addColumn(c, n.instance(), "relay", "boolean not null default false"); // likewise
        addColumn(c, n.instance(), "contracts", "jsonb"); // likewise; null: unknown
        addColumn(c, n.instance(), "routes", "text[]"); // likewise; null: unknown
        // after the upgrades, which add the columns some of them cover
        String deadDue = n.dead() + "_due";
        for (var table : objects.tables()) {
            for (var index : table.indexes()) {
                if (!index.name().equals(deadDue)) {
                    createIndex(c, table, index);
                } else if (!PartitionDdl.exists(c, deadDue)) {
                    // on a dead table with rows (kept forever by default) the build would hold dead inserts and
                    // outlast the statement_timeout: Maintenance builds it concurrently, and the pass claims by a
                    // scan meanwhile
                    if (PartitionDdl.hasRows(c, n.dead())) {
                        log.info("outbox index {} is missing on {}, which holds rows: maintenance builds it"
                                + " concurrently in its next run", deadDue, n.dead());
                    } else {
                        createIndex(c, table, index);
                    }
                }
            }
        }
    }

    /** One cursor per shard {@code 0..?-1} at the latest tick. */
    private static String insertCursors(TableNames n) {
        return "insert into " + n.cursor() + " (shard, tick_id) select s, (select max(id) from " + n.tick()
                + ") from generate_series(0, ? - 1) s";
    }

    /**
     * Fresh table: creates {@code premake} ranges and the tail. Existing table: first finishes a detach left pending
     * (an interrupted range drop's, which would fail the next one with 55000, or an operator's tail detach), then the
     * same extension maintenance runs: the tail swap, the one ACCESS EXCLUSIVE on the parent, taken without waiting
     * (and {@link Tuning#swapLockTimeout()}), after waiting up to {@code LOCK_TIMEOUT} for relay steps in
     * flight. A lock failure there (running transactions in a rolling deploy), or a swap the server cut at its
     * {@code statement_timeout} (a slow disk), rolls back whole and is logged and left to maintenance while inserts at
     * the current xid still find a partition. A hot tail is left to maintenance too:
     * turning it into a range scans it, which would hold the schema lock and every other instance's startup.
     * Partition DDL is serialised by {@code LOCK_MAINTENANCE}, which {@code holder} takes here after
     * {@code LOCK_SCHEMA}; {@link Maintenance} only tries it and never takes {@code LOCK_SCHEMA}, so the two can never
     * deadlock.
     */
    private void ensurePartitions(Connection c, TableNames n, LockHolder holder) throws SQLException {
        var partitions = XidPartitions.forBootstrap(n, config);
        partitions.warnIfActivityHidden(c); // once per start
        partitions.logTailConversion();
        try {
            var held = holder.lock(n.internalNamespace(), TableNames.LOCK_MAINTENANCE, LOCK_TIMEOUT);
            if (held == null) {
                throw new SQLException("could not take the maintenance lock of " + n.base() + " within "
                        + LOCK_TIMEOUT.toMillis() + " ms: a maintenance run holds it", LOCK_NOT_AVAILABLE);
            }
            try {
                partitions.finishPendingDetach(c, LOCK_TIMEOUT, LOCK_TIMEOUT);
                partitions.reown(c);
                partitions.ensurePremade(c, LOCK_TIMEOUT, LOCK_TIMEOUT);
            } finally {
                holder.release(held);
            }
        } catch (SQLException e) {
            var state = e.getSQLState();
            boolean denied = INSUFFICIENT_PRIVILEGE.equals(state); // as without USAGE on PL/pgSQL, the swap's DO block
            if (!(denied || LOCK_NOT_AVAILABLE.equals(state) || QUERY_CANCELED.equals(state))
                    || !partitions.acceptsInserts(c)) {
                throw e;
            }
            if (denied) { // fails alike on every run until an operator grants it: maintenance keeps reporting it
                log.error("outbox partition extension of {} at startup lacks a right of the owner role (SQLState {}):"
                        + " {}; inserts go to the attached tail until the right is granted", n.base(), state,
                        e.getMessage());
                return;
            }
            log.warn("outbox partition extension of {} gave up at startup (SQLState {}); maintenance retries it: {}",
                    n.base(), state, e.getMessage());
        }
    }

    private static void createIndex(Connection c, SchemaObjects.Table table, SchemaObjects.Index index)
            throws SQLException {
        if (PartitionDdl.exists(c, index.name())) {
            return; // CREATE INDEX IF NOT EXISTS would still queue a SHARE lock behind running writers
        }
        try {
            ddl(c, table.indexSql(index));
        } catch (SQLException e) {
            if (!DUPLICATE_TABLE.equals(e.getSQLState())) { // made meanwhile, by a dead-index build
                throw e;
            }
        }
    }

    /** Once per table: ACCESS EXCLUSIVE for a metadata-only change (a constant default or none). */
    private static void addColumn(Connection c, String table, String column, String definition) throws SQLException {
        if (!columnExists(c, table, column)) {
            ddl(c, "alter table " + table + " add column if not exists " + column + " " + definition);
        }
    }

    /**
     * A bootstrap DDL statement in a transaction of its own, under {@link #LOCK_TIMEOUT} and
     * {@link #STATEMENT_TIMEOUT} ({@code SET LOCAL}), fenced by the schema lock's holder.
     */
    private static void ddl(Connection c, String sql) throws SQLException {
        PartitionDdl.execute(c, LOCK_TIMEOUT, STATEMENT_TIMEOUT, sql);
    }

    /** {@code body}'s statements in one transaction, under the bounds of {@link #ddl}, committed after it. */
    private static <T> T bounded(Connection c, Sql.SqlFunction0<T> body) throws SQLException {
        return PartitionDdl.withTimeouts(c, LOCK_TIMEOUT, STATEMENT_TIMEOUT, body);
    }

    /** Checked first, as ALTER TABLE takes its lock even when the column exists. */
    static boolean columnExists(Connection c, String table, String column) throws SQLException {
        try (var ps = c.prepareStatement("select exists (select 1 from pg_attribute"
                + " where attrelid = to_regclass(?) and attname = ? and not attisdropped)")) {
            ps.setString(1, table);
            ps.setString(2, column);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    /**
     * Whether the error texts the components built from {@code config} store must be ASCII: true when the bootstrap
     * found a server encoding other than UTF8 or SQL_ASCII ({@link RetryStore}).
     */
    static boolean asciiText(OutboxConfig config) {
        return config instanceof Resolved r && r.asciiText();
    }

    /**
     * The shard count of the components built from {@code config}: the count {@link #start()} resolved when it is
     * {@link #config()}, else {@link Tuning#shards()}, as tests build them.
     */
    public static int shards(OutboxConfig config) {
        return config instanceof Resolved r ? r.shards() : Tuning.of(config).shards();
    }

    /**
     * The server encoding, read once: pgjdbc sends text in UTF8, which only UTF8 and SQL_ASCII (no conversion) take
     * whole; any other rejects a character it lacks with 22P05, so the error texts are then stored escaped.
     */
    private static boolean asciiText(Connection c, TableNames n) throws SQLException {
        String encoding;
        try (var st = c.createStatement(); var rs = st.executeQuery("show server_encoding")) {
            rs.next();
            encoding = rs.getString(1);
        }
        if (encoding.equals("UTF8") || encoding.equals("SQL_ASCII")) {
            return false;
        }
        log.warn("outbox {} is in a database with server_encoding {}, which lacks most of Unicode: the error texts it"
                + " stores (last_error, error, last_handler_error) have every non-ASCII character escaped as \\uXXXX;"
                + " a UTF8 database stores them as they are", n.base(), encoding);
        return true;
    }

    /**
     * Per-key order is id order, and ticks and cursor rows are ordered by id too: with {@code CACHE > 1} a session
     * draws ids from its own cached block, so a transaction that took a lock later can still get a lower id.
     */
    private static void checkSequenceCache(Connection c, TableNames n) throws SQLException {
        try (var ps = c.prepareStatement("select string_agg(format('%s has cache %s', s.seqrelid::regclass,"
                + " s.seqcache), ', ') from pg_sequence s where s.seqcache > 1 and s.seqrelid in ("
                + "pg_get_serial_sequence(?, 'id')::regclass, pg_get_serial_sequence(?, 'id')::regclass,"
                + " pg_get_serial_sequence(?, 'seq')::regclass)")) {
            ps.setString(1, n.base());
            ps.setString(2, n.tick());
            ps.setString(3, n.cursor());
            try (var rs = ps.executeQuery()) {
                rs.next();
                var cached = rs.getString(1);
                if (cached != null) {
                    throw new IllegalStateException("outbox identity sequence " + cached + ": cached ids are not"
                            + " assigned in lock order (each session draws from its own block), which breaks the"
                            + " order of a key's messages, of ticks and of cursor rows; set it back with"
                            + " ALTER SEQUENCE ... CACHE 1");
                }
            }
        }
    }

    /**
     * Moves an identity sequence that lags the ids its rows already hold past them, with an ERROR: logical replication
     * and DMS copy rows but not sequence values, and a cutover onto a cluster whose xid counter is ahead shows no xid
     * regression. A rewound tick id leaves every new tick below the newest one (no shard sees work again), a rewound
     * cursor seq leaves new cursor rows ignored (the same batch re-sent forever), a rewound outbox id reuses retry,
     * parked and dead ids (23505 on every step) and breaks key order. Bootstrap and every {@link Maintenance} run;
     * reads primary keys and the small tick and cursor tables: the outbox id goes by the ids the ticks recorded
     * ({@code max_id}), the cursors' {@code last_id} and the retry, parked and dead tables, and only when a sequence is
     * behind also by the outbox rows the newest tick may not cover ({@code xid >= xmin}, the top ranges alone), as the
     * rows written after it hold ids above all of those. On the owner's connection: {@code setval} needs UPDATE on the
     * sequence.
     */
    static void raiseRewoundSequences(Connection c, TableNames n) throws SQLException {
        // the outbox itself is named only in the second statement: a query naming it locks it (and every range) at
        // plan time, run or not, and would queue behind any ACCESS EXCLUSIVE request on it, a swap's say
        var rows = new ArrayList<String[]>(); // table, sequence, last value, held id
        try (var ps = c.prepareStatement("select t, s::text, coalesce(pg_sequence_last_value(s), 0), held from (values"
                + " (?::text, pg_get_serial_sequence(?, 'id')::regclass, greatest("
                + "(select max(max_id) from " + n.tick() + "), (select max(last_id) from " + n.cursor() + "),"
                + " (select max(id) from " + n.retry() + "), (select max(id) from " + n.parked() + "),"
                + " (select max(id) from " + n.dead() + "))),"
                + " (?::text, pg_get_serial_sequence(?, 'id')::regclass, (select max(id) from " + n.tick() + ")),"
                + " (?::text, pg_get_serial_sequence(?, 'seq')::regclass, (select max(seq) from " + n.cursor() + ")))"
                + " v(t, s, held)")) {
            ps.setString(1, n.base());
            ps.setString(2, n.base());
            ps.setString(3, n.tick());
            ps.setString(4, n.tick());
            ps.setString(5, n.cursor());
            ps.setString(6, n.cursor());
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new String[] {rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4)});
                }
            }
        }
        if (rows.stream().noneMatch(r -> r[3] != null && Long.parseLong(r[3]) > Long.parseLong(r[2]))) {
            return;
        }
        var raised = new ArrayList<String>();
        for (var b : rows) {
            long held = b[3] == null ? 0 : Long.parseLong(b[3]);
            if (b[0].equals(n.base())) {
                // the source's last rows, written after its newest tick, lie above every value above: the ranges that
                // tick may not cover, the top ones alone
                try (var ps = c.prepareStatement("select max(id) from " + n.base() + " where xid >= coalesce((select"
                        + " pg_snapshot_xmin(snapshot) from " + n.tick() + " order by id desc limit 1), '0'::xid8)");
                        var rs = ps.executeQuery()) {
                    if (rs.next() && rs.getLong(1) > held) {
                        held = rs.getLong(1);
                    }
                }
            }
            if (held <= Long.parseLong(b[2])) {
                continue;
            }
            try (var ps = c.prepareStatement("select setval(?::regclass, ?)")) {
                ps.setString(1, b[1]);
                ps.setLong(2, held);
                ps.execute();
            }
            raised.add(b[1] + " of " + b[0] + " from " + b[2] + " to " + held);
        }
        log.error("outbox identity sequences of {} were behind ids their rows already hold, and were moved"
                + " past them: {}. Logical replication and AWS DMS copy rows but not sequence values, so a"
                + " cutover onto another cluster leaves them at their start; without the move no new tick"
                + " would be seen, new cursor rows would be ignored and new outbox ids would collide with"
                + " retry, parked and dead rows. Sync the sequences as part of the cutover (README,"
                + " «Восстановление после регрессии xid»)", n.base(), String.join(", ", raised));
    }

    /**
     * The xid check {@link Maintenance} runs too: a halt found is recorded, then recovered as
     * {@link XidRecovery#autoRecover} decides, under {@code LOCK_MAINTENANCE}, which {@code holder} waits for up to
     * {@link #LOCK_TIMEOUT}; else an ERROR, and startup goes on halted. A halt whose ticks were repaired is left for
     * {@link Maintenance}'s check to lift.
     */
    private void checkXidCounter(Connection c, TableNames n, OutboxConfig resolved, LockHolder holder)
            throws SQLException {
        var recovery = new XidRecovery(n, resolved);
        var check = recovery.check(c);
        if (!check.halted()) {
            return;
        }
        if (check.recorded() == null) {
            recovery.recordHalt(c, check.tickXmax());
        }
        String why = XidRecovery.HALT_MODE;
        if (Tuning.of(config).xidRegression().equals("auto")) {
            why = XidRecovery.LOCK_BUSY;
            var held = holder.lock(n.internalNamespace(), TableNames.LOCK_MAINTENANCE, LOCK_TIMEOUT);
            if (held != null) {
                try {
                    why = recovery.autoRecover(c);
                } finally {
                    holder.release(held);
                }
            }
        }
        if (why != null) {
            log.error(recovery.halted(check, why));
        }
    }

    /**
     * Waits for other instances' bootstraps {@code schemaLockAttempts} times {@code LOCK_TIMEOUT}, then fails startup
     * naming the lock: a holder frozen mid-bootstrap must not hang every later startup.
     */
    private void lockSchema(LockHolder holder, TableNames n) throws SQLException {
        for (int attempt = 1; ; attempt++) {
            if (holder.lock(n.internalNamespace(), TableNames.LOCK_SCHEMA, LOCK_TIMEOUT) != null) {
                return;
            }
            var lock = "pg_advisory_xact_lock(" + n.internalNamespace() + ", " + TableNames.LOCK_SCHEMA + ")";
            if (attempt >= schemaLockAttempts) {
                throw new IllegalStateException("outbox bootstrap of " + n.base() + " gave up after "
                        + LOCK_TIMEOUT.multipliedBy(attempt).toSeconds() + " s waiting for its schema lock " + lock
                        + ", held by another session (another instance's bootstrap, possibly stuck); find it with"
                        + " select pid from pg_locks where locktype = 'advisory' and classid = "
                        + Integer.toUnsignedLong(n.internalNamespace()) + " and objid = " + TableNames.LOCK_SCHEMA
                        + " and objsubid = 2");
            }
            log.warn("outbox bootstrap of {} waits for its schema lock {}, held by another session (attempt {} of"
                    + " {})", n.base(), lock, attempt, schemaLockAttempts);
        }
    }
}
