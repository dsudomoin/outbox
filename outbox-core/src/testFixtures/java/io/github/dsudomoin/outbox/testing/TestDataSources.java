package io.github.dsudomoin.outbox.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The tests' Postgres, started once per JVM, and plain {@link DataSource}s over it, without any framework. 16 unless
 * {@code -Doutbox.test.postgres} names another image; {@code -Doutbox.test.containerCpus=1} gives it one CPU, as a
 * small CI runner would.
 */
public final class TestDataSources {

    private static final int CONNECT_SECONDS = 30;

    private TestDataSources() {}

    private static final class Pg {
        static final PostgreSQLContainer INSTANCE =
                new PostgreSQLContainer(System.getProperty("outbox.test.postgres", "postgres:16-alpine"))
                        .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=500")
                        .withCreateContainerCmdModifier(cmd -> {
                            var cpus = System.getProperty("outbox.test.containerCpus");
                            if (cpus != null) {
                                cmd.getHostConfig().withNanoCPUs((long) (Double.parseDouble(cpus) * 1e9));
                            }
                        });

        static {
            INSTANCE.start();
        }
    }

    /** Postgres 17, the first with {@code transaction_timeout}; started by the first test that needs it. */
    private static final class Pg17 {
        static final PostgreSQLContainer INSTANCE = new PostgreSQLContainer("postgres:17-alpine")
                .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=300");

        static {
            INSTANCE.start();
        }
    }

    public static String jdbcUrl() {
        return Pg.INSTANCE.getJdbcUrl();
    }

    public static String user() {
        return Pg.INSTANCE.getUsername();
    }

    public static String password() {
        return Pg.INSTANCE.getPassword();
    }

    /** The JDBC URL of the Postgres 17 server, for tests of what only 17 has. */
    public static String pg17JdbcUrl() {
        return Pg17.INSTANCE.getJdbcUrl();
    }

    public static String pg17User() {
        return Pg17.INSTANCE.getUsername();
    }

    public static String pg17Password() {
        return Pg17.INSTANCE.getPassword();
    }

    /** A new connection to the Postgres 17 server's database. */
    public static Connection pg17Connection() throws SQLException {
        return DriverManager.getConnection(pg17JdbcUrl(), pg17User(), pg17Password());
    }

    /** The container's address on its Docker network, where another container reaches it on port 5432. */
    static String containerAddress() {
        return Pg.INSTANCE.getContainerInfo().getNetworkSettings().getNetworks().values().iterator().next()
                .getIpAddress();
    }

    /** The database name in {@link #jdbcUrl()}. */
    static String databaseName() {
        return Pg.INSTANCE.getDatabaseName();
    }

    /** Connections of the test database whose sessions run in {@code zone}. */
    public static PGSimpleDataSource inZone(String zone) {
        var ds = new PGSimpleDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                var c = super.getConnection();
                TestSql.execute(c, "set time zone '" + zone + "'");
                return c;
            }
        };
        ds.setUrl(jdbcUrl());
        ds.setUser(user());
        ds.setPassword(password());
        return ds;
    }

    /** A new physical connection per {@code getConnection()}; one the server does not answer fails, not hangs the run. */
    public static PGSimpleDataSource newDataSource() {
        var ds = new PGSimpleDataSource();
        ds.setUrl(jdbcUrl());
        ds.setUser(user());
        ds.setPassword(password());
        ds.setConnectTimeout(CONNECT_SECONDS);
        ds.setLoginTimeout(CONNECT_SECONDS);
        return ds;
    }
}
