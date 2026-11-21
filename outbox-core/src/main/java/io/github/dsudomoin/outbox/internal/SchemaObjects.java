package io.github.dsudomoin.outbox.internal;

import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The base objects one version of the library needs, outside the xid and time ranges it makes at runtime: each table
 * with its columns, constraints, indexes and partition key, each tagged with the schema version that added it.
 * {@link io.github.dsudomoin.outbox.OutboxSql} renders them, {@link OutboxSchema} and {@link PgInbox} create them
 * from the same text, the startup check without bootstrap checks them, and a test holds both to what the bootstrap
 * creates. Names as {@link TableNames} gives them, unresolved.
 */
public record SchemaObjects(List<Table> tables) {

    private static final String TIMESTAMPTZ = "timestamp with time zone";

    /**
     * {@code partitionKey}: {@code range (...)}, null for a plain table; {@code constraints}: inside the
     * {@code create table}; {@code checks}: {@code alter table ... add constraint ... not valid} after it, as the
     * bootstrap adds them.
     */
    public record Table(String name, int since, @Nullable String partitionKey, List<Column> columns,
            List<String> constraints, List<Check> checks, List<Index> indexes) {

        /** The {@code create table} of its columns, constraints and partition key, without its checks or indexes. */
        public String createSql(boolean ifNotExists) {
            var lines = columns.stream().map(SchemaObjects::columnSql).collect(Collectors.toList());
            lines.addAll(constraints);
            return "create table " + (ifNotExists ? "if not exists " : "") + name + " (\n"
                    + lines.stream().map(line -> "    " + line).collect(Collectors.joining(",\n")) + "\n)"
                    + (partitionKey != null ? " partition by " + partitionKey : "");
        }

        /** The {@code create index} of {@code index}, one of this table's. */
        public String indexSql(Index index) {
            return "create index " + index.name().substring(index.name().indexOf('.') + 1) + " on " + name + ' '
                    + index.definition();
        }
    }

    /** {@code type} as {@code format_type} prints it; an identity column is {@code generated always}, cache 1. */
    public record Column(String name, String type, boolean notNull, @Nullable String defaultValue, boolean identity,
            int since) {}

    /** {@code definition}: after {@code on <table>}. */
    public record Index(String name, String definition, int since) {}

    /** {@code name}: unqualified. */
    public record Check(String name, String definition) {}

    public static SchemaObjects outbox(TableNames n) {
        var message = List.of(
                column("message_id", "uuid", true),
                column("topic", "text", true),
                column("key", "bytea", false),
                column("payload", "bytea", false),
                column("headers", "bytea", false));
        return new SchemaObjects(List.of(
                new Table(n.base(), 1, "range (xid)", concat(List.of(
                        identity("id"),
                        column("shard", "smallint", true),
                        column("xid", "xid8", true, "pg_current_xact_id()"),
                        column("created_at", TIMESTAMPTZ, true, "clock_timestamp()")), message),
                        List.of(), List.of(), List.of(index(n.base() + "_shard_xid", "(shard, xid)"))),
                // the xmax index: Maintenance's xid check
                plain(n.tick(), List.of(
                        identity("id"),
                        column("snapshot", "pg_snapshot", true),
                        column("created_at", TIMESTAMPTZ, true, "clock_timestamp()"),
                        column("max_id", "bigint", false),
                        column("dirty", "bit varying", false),
                        column("shards", "smallint[]", false)),
                        List.of("primary key (id)"), List.of(),
                        List.of(index(n.tick() + "_xmax", "((pg_snapshot_xmax(snapshot)))"))),
                plain(n.cursor(), List.of(
                        column("shard", "smallint", true),
                        identity("seq"),
                        column("tick_id", "bigint", true),
                        column("target_tick_id", "bigint", false),
                        column("last_id", "bigint", true, "0"),
                        column("paused_until", TIMESTAMPTZ, false),
                        column("failures", "integer", true, "0"),
                        column("retry_due_at", TIMESTAMPTZ, false),
                        column("parked_due_at", TIMESTAMPTZ, false)),
                        List.of("primary key (shard, seq)"), List.of(), List.of()),
                // maintenance's checkpoint, which moves idle shards on without cursor rows (Cursors); none until its
                // run
                plain(n.checkpoint(), List.of(
                        identity("seq"),
                        column("tick_id", "bigint", true),
                        column("busy", "bit varying", true),
                        column("busy_tick_id", "bigint", true),
                        column("created_at", TIMESTAMPTZ, true, "clock_timestamp()")),
                        List.of("primary key (seq)"), List.of(), List.of()),
                plain(n.retry(), concat(List.of(column("id", "bigint", true), column("shard", "smallint", true)),
                        message, List.of(
                                column("created_at", TIMESTAMPTZ, true),
                                column("attempts", "integer", true, "0"),
                                column("next_attempt_at", TIMESTAMPTZ, false),
                                column("last_error", "text", false),
                                column("requeues", "integer", true, "0"),
                                column("waiting_since", TIMESTAMPTZ, false))),
                        List.of("primary key (id)"), List.of(), List.of(
                                index(n.retry() + "_key", "(shard, topic, key, id)"),
                                index(n.retry() + "_due",
                                        "(shard, next_attempt_at) where next_attempt_at is not null"),
                                index(n.retry() + "_waiting", "(waiting_since) where waiting_since is not null"))),
                // requeued records: unlike retry rows, they do not block their key
                plain(n.parked(), concat(List.of(column("id", "bigint", true), column("shard", "smallint", true)),
                        message, List.of(
                                column("created_at", TIMESTAMPTZ, true),
                                column("attempts", "integer", true, "0"),
                                column("requeues", "integer", true, "0"),
                                column("due_at", TIMESTAMPTZ, true))),
                        List.of("primary key (id)"), List.of(), List.of(
                                index(n.parked() + "_key", "(shard, topic, key, id)"),
                                index(n.parked() + "_due", "(shard, due_at)"))),
                plain(n.dead(), concat(List.of(column("id", "bigint", true), column("shard", "smallint", true)),
                        message, List.of(
                                column("created_at", TIMESTAMPTZ, true),
                                column("attempts", "integer", true),
                                column("failed_at", TIMESTAMPTZ, true, "now()"),
                                column("error", "text", false),
                                column("status", "text", true, "'kept'"),
                                column("due_at", TIMESTAMPTZ, false),
                                column("handler_attempts", "integer", true, "0"),
                                column("last_handler_error", "text", false),
                                column("redrives", "integer", true, "0"))),
                        List.of("primary key (id)"),
                        List.of(new Check(n.unqualified(n.dead()) + "_status",
                                "check (status in ('pending', 'kept', 'scheduled'))")),
                        List.of(index(n.dead() + "_due", "(due_at) where due_at is not null"))),
                // how many times a message was redriven, read by its next dead row
                plain(n.redriven(), List.of(
                        column("message_id", "uuid", true),
                        column("redrives", "integer", true),
                        column("at", TIMESTAMPTZ, true, "now()")),
                        List.of("primary key (message_id)"), List.of(), List.of()),
                // the xid-regression halt, persisted by Maintenance: one row while halted; recovery_failed: why the
                // automatic recovery stopped trying (XidRecovery#autoRecover)
                plain(n.halt(), List.of(
                        column("id", "integer", true, "1"),
                        column("halted_at", TIMESTAMPTZ, true, "now()"),
                        column("max_tick_xmax", "xid8", true),
                        column("recovery_failed", "text", false)),
                        List.of("primary key (id)", "check (id = 1)"), List.of(), List.of()),
                // when a maintenance run last completed every step, on any instance
                plain(n.maintenance(), List.of(
                        column("id", "integer", true, "1"),
                        column("succeeded_at", TIMESTAMPTZ, true),
                        column("tail_oversized_since", TIMESTAMPTZ, false)),
                        List.of("primary key (id)", "check (id = 1)"), List.of(), List.of()),
                // PgOutboxAdmin#recoverStrandedShards: the tick each stranded shard's repair goes to, while it is
                // stranded at stranded_tick_id with its latest cursor row on cursor_tick_id and cursor_target_tick_id
                plain(n.stranded(), List.of(
                        column("shard", "smallint", true),
                        column("stranded_tick_id", "bigint", true),
                        column("cursor_tick_id", "bigint", true),
                        column("cursor_target_tick_id", "bigint", false),
                        column("tick_id", "bigint", true)),
                        List.of("primary key (shard)"), List.of(), List.of()),
                // XidPartitions: the ranges it detaches as relayed, by unqualified name, until they are dropped
                plain(n.dropping(), List.of(
                        column("table_name", "text", true),
                        column("at", TIMESTAMPTZ, true, "now()")),
                        List.of("primary key (table_name)"), List.of(), List.of()),
                // Heartbeat: the instances running, by the database clock, for the reshard guard and the admin
                // status; a row is live until its expires_at, which its writer sets from its own maintenanceInterval
                plain(n.instance(), List.of(
                        column("instance_id", "text", true),
                        column("shards", "integer", true),
                        column("last_seen", TIMESTAMPTZ, true, "now()"),
                        column("hook_active", "boolean", true, "false"),
                        column("expires_at", TIMESTAMPTZ, false),
                        column("relay", "boolean", true, "false"),
                        column("contracts", "jsonb", false),
                        column("routes", "text[]", false)),
                        List.of("primary key (instance_id)"), List.of(), List.of())));
    }

    /** {@code n}: the inbox table's names. */
    public static SchemaObjects inbox(TableNames n) {
        return new SchemaObjects(List.of(new Table(n.base(), 1, "range (message_id)", List.of(
                column("message_id", "uuid", true),
                column("consumer", "text", true, "''"),
                column("received_at", TIMESTAMPTZ, true, "now()")),
                List.of("primary key (message_id, consumer)"), List.of(), List.of())));
    }

    private static Table plain(String name, List<Column> columns, List<String> constraints, List<Check> checks,
            List<Index> indexes) {
        return new Table(name, 1, null, columns, constraints, checks, indexes);
    }

    private static Column identity(String name) {
        return new Column(name, "bigint", true, null, true, 1);
    }

    private static Column column(String name, String type, boolean notNull) {
        return new Column(name, type, notNull, null, false, 1);
    }

    private static Column column(String name, String type, boolean notNull, String defaultValue) {
        return new Column(name, type, notNull, defaultValue, false, 1);
    }

    private static Index index(String name, String definition) {
        return new Index(name, definition, 1);
    }

    private static String columnSql(Column column) {
        var line = new StringBuilder(column.name()).append(' ').append(column.type());
        if (column.identity()) {
            line.append(" generated always as identity (cache 1)"); // cached ids break the order of a key's messages
        } else if (column.notNull()) {
            line.append(" not null");
        }
        if (column.defaultValue() != null) {
            line.append(" default ").append(column.defaultValue());
        }
        return line.toString();
    }

    @SafeVarargs
    private static List<Column> concat(List<Column>... parts) {
        return Arrays.stream(parts).flatMap(List::stream).toList();
    }
}
