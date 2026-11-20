package io.github.dsudomoin.outbox.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

/**
 * pgbouncer in transaction mode in front of {@link TestDataSources}' Postgres, as a platform runs it for a service whose
 * only way to the database it is: every client served by {@code serverConnections} server sessions per role, handed
 * out round robin ({@code server_round_robin = 1}), so consecutive autocommit statements of one client land in
 * different server sessions once the pool is warm ({@link #warm}); whatever a statement leaves in its session reaches
 * other clients. {@code maxPreparedStatements} 0 keeps no prepared statement across server sessions (pgbouncer before
 * 1.21, or one configured so), which needs {@code prepareThreshold=0} on the client. Logins of any role: pgbouncer
 * reads its password from the server as the container's superuser. {@code -Doutbox.test.pgbouncer} names another
 * image.
 */
public final class TestPooler implements AutoCloseable {

    private static final int PORT = 6432;
    /**
     * Settings a session of pgbouncer's may carry that are no client's leftovers: pgbouncer sets these itself when it
     * hands a server session to another client.
     */
    private static final String POOLER_OWN = "'application_name', 'client_encoding', 'DateStyle', 'TimeZone',"
            + " 'IntervalStyle', 'standard_conforming_strings', 'extra_float_digits'";

    private final GenericContainer<?> container;
    private final int serverConnections;

    private TestPooler(GenericContainer<?> container, int serverConnections) {
        this.container = container;
        this.serverConnections = serverConnections;
    }

    /** Started; the caller closes it. */
    public static TestPooler start(int serverConnections, int maxPreparedStatements) {
        var ini = """
                [databases]
                * = host=%s port=5432
                [pgbouncer]
                listen_addr = 0.0.0.0
                listen_port = %d
                auth_type = scram-sha-256
                auth_file = /etc/pgbouncer/userlist.txt
                auth_user = %s
                pool_mode = transaction
                default_pool_size = %d
                max_client_conn = 1000
                server_round_robin = 1
                max_prepared_statements = %d
                query_wait_timeout = 30
                ignore_startup_parameters = extra_float_digits
                """.formatted(TestDataSources.containerAddress(), PORT, TestDataSources.user(), serverConnections,
                maxPreparedStatements);
        var users = "\"" + TestDataSources.user() + "\" \"" + TestDataSources.password() + "\"\n";
        @SuppressWarnings("resource") // closed by close()
        var c = new GenericContainer<>(System.getProperty("outbox.test.pgbouncer", "edoburu/pgbouncer:v1.26.1-p0"))
                .withCopyToContainer(Transferable.of(ini), "/etc/pgbouncer/pgbouncer.ini")
                .withCopyToContainer(Transferable.of(users), "/etc/pgbouncer/userlist.txt")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/usr/bin/pgbouncer"))
                .withCommand("/etc/pgbouncer/pgbouncer.ini")
                .withExposedPorts(PORT)
                .waitingFor(Wait.forLogMessage(".*process up.*", 1));
        c.start();
        return new TestPooler(c, serverConnections);
    }

    /** The URL of the database through pgbouncer, without connection properties. */
    public String jdbcUrl() {
        return "jdbc:postgresql://" + container.getHost() + ":" + container.getMappedPort(PORT) + "/"
                + TestDataSources.databaseName();
    }

    public int serverConnections() {
        return serverConnections;
    }

    /** A new connection through pgbouncer as {@code user}, whose password is {@code password}. */
    public Connection connect(String user, String password, Properties properties) throws SQLException {
        var p = new Properties();
        p.putAll(properties);
        p.setProperty("user", user);
        p.setProperty("password", password);
        p.setProperty("connectTimeout", "30");
        p.setProperty("socketTimeout", "60");
        p.setProperty("prepareThreshold", p.getProperty("prepareThreshold", "0"));
        return DriverManager.getConnection(jdbcUrl(), p);
    }

    /**
     * Opens every server session of {@code user}'s pool, so round robin has them all to hand out from then on: as many
     * clients as the pool has sessions each hold a transaction at once.
     */
    public void warm(String user, String password) throws SQLException {
        sessions(user, password, c -> {});
    }

    /**
     * What clients left in the server sessions of {@code user}'s pool, empty when nothing: session advisory locks and
     * settings set for the session (not pgbouncer's own), read in each session by as many clients holding a transaction
     * at once as the pool has sessions. The caller must have no transaction of its own open through the pool.
     */
    public List<String> leftovers(String user, String password) throws SQLException {
        var found = new ArrayList<String>();
        sessions(user, password, c -> {
            try (var st = c.createStatement(); var rs = st.executeQuery("select pg_backend_pid(),"
                    + " (select string_agg(format('advisory lock (%s, %s, %s) %s', classid, objid, objsubid, mode), ', ')"
                    + " from pg_locks where locktype = 'advisory' and pid = pg_backend_pid()),"
                    + " (select string_agg(name || ' = ' || setting, ', ') from pg_settings where source = 'session'"
                    + " and name not in (" + POOLER_OWN + "))")) {
                rs.next();
                for (int i = 2; i <= 3; i++) {
                    if (rs.getString(i) != null) {
                        found.add("server session " + rs.getInt(1) + ": " + rs.getString(i));
                    }
                }
            }
        });
        return found;
    }

    private interface SessionCheck {
        void accept(Connection c) throws SQLException;
    }

    private void sessions(String user, String password, SessionCheck check) throws SQLException {
        var open = new ArrayList<Connection>();
        try {
            var seen = new java.util.HashSet<Integer>();
            for (int i = 0; i < serverConnections; i++) {
                var c = connect(user, password, new Properties());
                open.add(c);
                c.setAutoCommit(false); // holds its server session until the commit below
                try (var st = c.createStatement(); var rs = st.executeQuery("select pg_backend_pid()")) {
                    rs.next();
                    if (!seen.add(rs.getInt(1))) {
                        throw new IllegalStateException("pgbouncer gave two clients in transactions one session");
                    }
                }
                check.accept(c);
            }
        } finally {
            for (var c : open) {
                try (c) {
                    c.rollback();
                }
            }
        }
    }

    @Override
    public void close() {
        container.close();
    }
}
