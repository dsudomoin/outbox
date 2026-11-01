package io.github.dsudomoin.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The startup check of base objects the application's migrations own ({@code outbox.schema.bootstrap = false}): every
 * table, column (type, nullability, default expression as {@code pg_get_expr} prints it, identity), index (its table,
 * method, key with its order and operator classes, predicate, validity), primary key (its columns, not deferrable) and
 * partition key of {@link SchemaObjects} as this version needs it; DML rights on every table (and {@code SELECT} on
 * the outbox's id sequence) for the role of {@code app}; and for the role of {@code ddl}, which makes and drops the
 * partitions, membership of the partitioned table's owner and {@code CREATE} on its schema. With an owner source, a
 * WARN when the application role may create where the owner's sessions resolve functions. What it does not know, a
 * newer schema's columns, indexes, tables and defaults, it ignores. Fails with one {@link IllegalStateException}
 * naming every problem.
 */
final class SchemaCheck {

    private static final Logger log = LoggerFactory.getLogger(SchemaCheck.class);
    private static final List<String> DML = List.of("SELECT", "INSERT", "UPDATE", "DELETE");
    private static final String PRIMARY_KEY = "primary key ";
    /** In what {@code pg_get_indexdef} prints: what the application's migration may choose for an index. */
    private static final Pattern OPTIONAL = Pattern.compile(" (?:INCLUDE|WITH) \\([^)]*\\)| TABLESPACE \\S+");

    private SchemaCheck() {}

    /** {@code table}: the configured name, for the message; {@code render}: the method whose SQL fixes it. */
    static void run(Connection app, Connection ddl, SchemaObjects required, String table, String render)
            throws SQLException {
        var problems = new ArrayList<String>();
        for (var t : required.tables()) {
            Long oid = oid(app, t.name());
            if (oid == null) {
                problems.add("table " + t.name() + " is missing" + fix(t.since(), render));
                continue;
            }
            if (t.partitionKey() != null) {
                var key = string(app, "select pg_get_partkeydef(?::oid)", oid);
                if (key == null || !key.toLowerCase(Locale.ROOT).equals(t.partitionKey())) {
                    problems.add(t.name() + " is " + (key == null ? "not partitioned" : "partitioned by " + key)
                            + ", not by " + t.partitionKey());
                }
            }
            columns(app, oid, t, render, problems);
            for (var index : t.indexes()) {
                index(app, oid, t, index, render, problems);
            }
            primaryKey(app, oid, t, render, problems);
            var missing = new ArrayList<String>();
            for (var privilege : DML) {
                if (!bool(app, "select has_table_privilege(?::oid, '" + privilege + "')", oid)) {
                    missing.add(privilege);
                }
            }
            if (!missing.isEmpty()) {
                problems.add(role(app) + " lacks " + String.join(", ", missing) + " on " + t.name());
            }
            if (t.partitionKey() != null) {
                partitionRights(ddl, t.name(), problems);
            }
        }
        var sequence = sequenceOf(app, required);
        if (sequence != null && !bool(app, "select has_sequence_privilege(?::oid, 'SELECT')", sequence.oid())) {
            problems.add(role(app) + " lacks SELECT on sequence " + sequence.name());
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("outbox schema check of " + table + " failed (outbox.schema.bootstrap ="
                    + " false): " + String.join("; ", problems));
        }
    }

    /**
     * Fails unless {@code app} and {@code owner} reach one database of one server, where {@code table} names the same
     * table (or none on both): an owner source pointed elsewhere would make partitions where the application never
     * writes.
     */
    static void requireSameDatabase(Connection app, Connection owner, String table) throws SQLException {
        var a = where(app, table);
        var o = where(owner, table);
        if (!a.equals(o)) {
            throw new IllegalStateException("outbox owner connection source reaches another database than the"
                    + " application's: owner " + o + ", application " + a + "; point both at the database of " + table
                    + " (and, for an unqualified name, give both roles the same search_path)");
        }
    }

    /**
     * Warns when the role of {@code app}, without the rights of {@code owner}'s role, may create in a schema the
     * sessions of {@code owner} search for functions and operators: an overload of a built-in for {@code varchar}, the
     * type a bound string has, made there takes over this library's queries on {@code owner} and runs with the owner's
     * rights (CVE-2018-1058), so the application role would not be DML-only. Names everything through
     * {@code pg_catalog} itself.
     */
    static void warnIfAppCanCreateWhereOwnerResolves(Connection app, Connection owner, String table)
            throws SQLException {
        String role;
        try (var st = app.createStatement(); var rs = st.executeQuery("select current_user")) {
            rs.next();
            role = rs.getString(1);
        }
        var schemas = new ArrayList<String>();
        var revokes = new ArrayList<String>();
        String ownerRole = null;
        String appRole = null;
        try (var ps = owner.prepareStatement("select pg_catalog.quote_ident(s), pg_catalog.quote_ident(current_user),"
                + " pg_catalog.quote_ident(?::text) from pg_catalog.unnest(pg_catalog.current_schemas(false)) s"
                + " where pg_catalog.has_schema_privilege(?::name, s::text, 'CREATE')"
                + " and not pg_catalog.pg_has_role(?::name, current_user, 'MEMBER')")) {
            ps.setString(1, role);
            ps.setString(2, role);
            ps.setString(3, role);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    schemas.add(rs.getString(1));
                    ownerRole = rs.getString(2);
                    appRole = rs.getString(3);
                    revokes.add("REVOKE CREATE ON SCHEMA " + rs.getString(1) + " FROM PUBLIC, " + appRole);
                }
            }
        }
        if (!schemas.isEmpty()) {
            log.warn("outbox {}: role {} can CREATE in schema {}, which the sessions of {} (the owner connection"
                    + " source) search for functions and operators: what {} creates there can run with the rights of"
                    + " {}. Revoke it (or from the role {} inherits it through): {}", table, appRole,
                    String.join(", ", schemas), ownerRole, appRole, ownerRole, appRole, String.join("; ", revokes));
        }
    }

    /**
     * Where {@code c} reaches, as text two connections compare: the database, the server's start and what
     * {@code table} resolves to. Only that resolution may differ by session (by its {@code search_path}, on purpose),
     * so the start is written in UTC: in the session's {@code TimeZone} one server would read as two.
     */
    static String where(Connection c, String table) throws SQLException {
        try (var ps = c.prepareStatement("select current_database(), to_char(pg_postmaster_start_time()"
                + " at time zone 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), to_regclass(?)::oid")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                var oid = rs.getString(3);
                return "database " + rs.getString(1) + " of the server started " + rs.getString(2) + " UTC, "
                        + (oid == null ? "no table " + table : "table " + table + " oid " + oid);
            }
        }
    }

    private static void columns(Connection c, long oid, SchemaObjects.Table t, String render, List<String> problems)
            throws SQLException {
        record Actual(String type, boolean notNull, @Nullable String defaultValue, boolean identity) {}
        var actual = new HashMap<String, Actual>();
        try (var ps = c.prepareStatement("select a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,"
                + " pg_get_expr(d.adbin, d.adrelid), a.attidentity <> '' from pg_attribute a left join pg_attrdef d"
                + " on d.adrelid = a.attrelid and d.adnum = a.attnum where a.attrelid = ?::oid and a.attnum > 0"
                + " and not a.attisdropped")) {
            ps.setLong(1, oid);
            try (var rs = ps.executeQuery()) {
                while (rs.next()) {
                    actual.put(rs.getString(1), new Actual(rs.getString(2), rs.getBoolean(3), rs.getString(4),
                            rs.getBoolean(5)));
                }
            }
        }
        for (var column : t.columns()) {
            var name = t.name() + "." + column.name();
            var a = actual.get(column.name());
            if (a == null) {
                problems.add("column " + name + " is missing" + fix(column.since(), render));
                continue;
            }
            if (!a.type().equals(column.type())) {
                problems.add("column " + name + " is " + a.type() + ", not " + column.type());
            }
            if (a.notNull() && !column.notNull()) { // the library leaves it null; the reverse is a newer schema's
                problems.add("column " + name + " is not null, but this version leaves it null");
            }
            if (column.identity() && !a.identity()) {
                problems.add("column " + name + " is not an identity column" + fix(column.since(), render));
            } else if (column.defaultValue() != null && a.defaultValue() == null) {
                problems.add("column " + name + " has no default (" + column.defaultValue() + ")"
                        + fix(column.since(), render));
            } else if (column.defaultValue() != null && !a.defaultValue().equals(column.defaultValue())
                    // as Postgres prints a literal: '' is ''::text
                    && !a.defaultValue().equals(column.defaultValue() + "::" + column.type())) {
                problems.add("column " + name + " has default " + a.defaultValue() + ", not " + column.defaultValue());
            }
        }
    }

    /**
     * {@code index} exists, on table {@code oid}, valid, not unique, with the required method, key and predicate.
     * Storage options, included columns and the tablespace are the migration's to choose.
     */
    private static void index(Connection c, long oid, SchemaObjects.Table t, SchemaObjects.Index index, String render,
            List<String> problems) throws SQLException {
        Long own = oid(c, index.name());
        if (own == null) {
            problems.add("index " + index.name() + " is missing" + fix(index.since(), render));
            return;
        }
        try (var ps = c.prepareStatement("select i.indrelid = ?::oid, i.indrelid::regclass::text, i.indisunique,"
                + " i.indisvalid, pg_get_indexdef(i.indexrelid, 0, true) from pg_index i where i.indexrelid = ?::oid")) {
            ps.setLong(1, oid);
            ps.setLong(2, own);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    problems.add(index.name() + " is no index");
                    return;
                }
                if (!rs.getBoolean(1)) {
                    problems.add("index " + index.name() + " is on " + rs.getString(2) + ", not on " + t.name());
                    return;
                }
                if (rs.getBoolean(3)) {
                    problems.add("index " + index.name() + " is unique, and this version needs it not to be");
                }
                if (!rs.getBoolean(4) && !index.rebuilt()) {
                    problems.add("index " + index.name() + " is INVALID, as an interrupted build leaves it: drop it"
                            + " and build it again");
                }
                var actual = definition(rs.getString(5));
                var required = "btree " + index.definition();
                if (!canonical(actual).equals(canonical(required))) {
                    problems.add("index " + index.name() + " is " + actual + ", not " + required);
                }
            }
        }
    }

    /** What {@code pg_get_indexdef} prints after {@code USING}, without what is the migration's to choose. */
    private static String definition(String indexdef) {
        return OPTIONAL.matcher(indexdef.substring(indexdef.indexOf(" USING ") + " USING ".length())).replaceAll("");
    }

    /** The primary key {@code t} requires, if any, on exactly its columns: an upsert's conflict target needs them. */
    private static void primaryKey(Connection c, long oid, SchemaObjects.Table t, String render, List<String> problems)
            throws SQLException {
        var required = t.constraints().stream().filter(constraint -> constraint.startsWith(PRIMARY_KEY)).findFirst();
        if (required.isEmpty()) {
            return;
        }
        var actual = string(c, "select (select string_agg(pg_get_indexdef(i.indexrelid, k, true), ', ' order by k) from"
                + " pg_index i, generate_series(1, i.indnkeyatts) k where i.indrelid = ?::oid and i.indisprimary)",
                oid); // one row, null without a primary key
        var columns = required.get().substring(PRIMARY_KEY.length());
        if (actual == null) {
            problems.add(t.name() + " has no primary key" + fix(t.since(), render));
        } else if (!canonical(actual).equals(canonical(columns))) {
            problems.add(t.name() + " has primary key (" + actual + "), not " + columns);
        } else if (bool(c, "select exists (select 1 from pg_constraint where conrelid = ?::oid and contype = 'p' and"
                + " condeferrable)", oid)) {
            problems.add(t.name() + " has a deferrable primary key, which on conflict cannot use");
        }
    }

    /** But for what Postgres adds when it prints a definition: parentheses, quotes, case and spacing. */
    private static String canonical(String definition) {
        return definition.toLowerCase(Locale.ROOT).replaceAll("[()\"\\s]", "");
    }

    /** Partition DDL needs the parent's owner role (or membership of it) and {@code CREATE} on its schema. */
    private static void partitionRights(Connection ddl, String table, List<String> problems) throws SQLException {
        try (var ps = ddl.prepareStatement("select pg_has_role(r.relowner, 'USAGE'), has_schema_privilege("
                + "r.relnamespace, 'CREATE'), pg_get_userbyid(r.relowner), n.nspname from pg_class r join"
                + " pg_namespace n on n.oid = r.relnamespace where r.oid = to_regclass(?)")) {
            ps.setString(1, table);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return; // missing, as the application connection already said
                }
                if (!rs.getBoolean(1)) {
                    problems.add(role(ddl) + " does not own " + table + ", which its partition DDL needs (owner "
                            + rs.getString(3) + ": make this role a member of it, or give the outbox a connection"
                            + " source of the owner)");
                }
                if (!rs.getBoolean(2)) {
                    problems.add(role(ddl) + " lacks CREATE on schema " + rs.getString(4) + ", where the partitions"
                            + " of " + table + " are made");
                }
            }
        }
    }

    private record Sequence(String name, long oid) {}

    /** The outbox's id sequence, which the relay's ticks read; null for the inbox or when its table is missing. */
    private static Sequence sequenceOf(Connection c, SchemaObjects required) throws SQLException {
        var base = required.tables().get(0);
        if (base.columns().stream().noneMatch(SchemaObjects.Column::identity)) {
            return null;
        }
        try (var ps = c.prepareStatement("select s, s::regclass::oid from pg_get_serial_sequence(?, 'id') s"
                + " where to_regclass(?) is not null")) {
            ps.setString(1, base.name());
            ps.setString(2, base.name());
            try (var rs = ps.executeQuery()) {
                return rs.next() && rs.getString(1) != null ? new Sequence(rs.getString(1), rs.getLong(2)) : null;
            }
        }
    }

    private static String fix(int since, String render) {
        return " (schema version " + since + ": apply " + render + ")";
    }

    private static String role(Connection c) throws SQLException {
        return "role " + string(c, "select current_user", null);
    }

    private static Long oid(Connection c, String name) throws SQLException {
        try (var ps = c.prepareStatement("select to_regclass(?)::oid")) {
            ps.setString(1, name);
            try (var rs = ps.executeQuery()) {
                rs.next();
                long oid = rs.getLong(1);
                return rs.wasNull() ? null : oid;
            }
        }
    }

    private static boolean bool(Connection c, String sql, long oid) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            ps.setLong(1, oid);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    private static String string(Connection c, String sql, Long oid) throws SQLException {
        try (var ps = c.prepareStatement(sql)) {
            if (oid != null) {
                ps.setLong(1, oid);
            }
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }
}
