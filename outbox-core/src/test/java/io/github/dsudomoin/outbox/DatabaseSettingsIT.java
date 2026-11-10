package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * What a DBA sets on the server, not the application: limits on the role shorter than a handler call, and a node that
 * turns read-only under the fleet (a failover pointing the pool at a standby, {@code default_transaction_read_only}).
 * Postgres 17, the first with {@code transaction_timeout}.
 */
class DatabaseSettingsIT {

    private static final class Pg {
        static final PostgreSQLContainer INSTANCE = new PostgreSQLContainer("postgres:17-alpine")
                .withCommand("postgres", "-c", "fsync=off", "-c", "max_connections=300");

        static {
            INSTANCE.start();
        }
    }

    private final List<AutoCloseable> closing = new ArrayList<>();

    @AfterEach
    void close() throws Exception {
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
    }

    private static Connection superuser(String database) throws SQLException {
        var url = Pg.INSTANCE.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + database + "$1");
        return DriverManager.getConnection(url, Pg.INSTANCE.getUsername(), Pg.INSTANCE.getPassword());
    }

    private HikariDataSource pool(String database, String user, String password, int size) {
        var hikari = new HikariConfig();
        hikari.setJdbcUrl(Pg.INSTANCE.getJdbcUrl().replaceFirst("/[^/?]+(\\?|$)", "/" + database + "$1"));
        hikari.setUsername(user);
        hikari.setPassword(password);
        hikari.setMaximumPoolSize(size);
        hikari.setConnectionTimeout(5_000);
        hikari.addDataSourceProperty("socketTimeout", "30");
        var ds = new HikariDataSource(hikari);
        closing.add(ds);
        return ds;
    }

    /**
     * Every limit a DBA may set on the role, all far shorter than a handler call of 3.6 s and the dead-letter hook's
     * 1.5 s: the relay's step, the dead-letter pass, maintenance, the heartbeat, the inbox and the admin keep working,
     * each message delivered exactly once, no step failing.
     */
    @Test
    void limitsOnTheRoleShorterThanAHandlerCallCutNothing() throws Exception {
        var role = "lim_" + UUID.randomUUID().toString().substring(0, 8);
        try (var c = superuser("test")) {
            TestSql.execute(c, "create role " + role + " login password 'lim'");
            TestSql.execute(c, "create schema " + role + " authorization " + role);
            for (var setting : List.of("statement_timeout = '500ms'", "idle_in_transaction_session_timeout = '1s'",
                    "transaction_timeout = '1s'", "lock_timeout = '200ms'", "idle_session_timeout = '3s'")) {
                TestSql.execute(c, "alter role " + role + " set " + setting);
            }
        }
        var ds = pool("test", role, "lim", 12);
        var transactions = new JdbcTransactions(ds);
        var config = new TestOutboxConfig().table(role + ".outbox").inboxTable(role + ".inbox").shards(2)
                .batchTimeout(Duration.ofSeconds(10)).retry(Duration.ofMillis(200), Duration.ofSeconds(1), 2)
                .maintenanceInterval(Duration.ofMillis(500));
        var inbox = InboxRuntime.builder(config, transactions).build();
        closing.add(inbox);
        inbox.start();
        Map<UUID, AtomicInteger> calls = new ConcurrentHashMap<>();
        Set<UUID> effects = ConcurrentHashMap.newKeySet();
        var failures = new CopyOnWriteArrayList<Throwable>();
        var hooked = new CopyOnWriteArrayList<UUID>();
        var runtime = OutboxRuntime.builder(config, transactions, record -> {
            calls.computeIfAbsent(record.messageId(), id -> new AtomicInteger()).incrementAndGet();
            switch (record.topic()) {
                case "poison" -> throw new IllegalStateException("poison");
                case "slow" -> Thread.sleep(1200); // three in a call: 3.6 s, the step's transaction idle all along
                default -> {}
            }
            if (transactions.inTx(c -> inbox.inbox().tryAccept(record.messageId()))) {
                effects.add(record.messageId());
            }
        }).deadLetterHandler(letters -> {
            Thread.sleep(1500); // the pass's claim stays idle meanwhile
            letters.forEach(l -> hooked.add(l.messageId()));
            return letters.stream().map(l -> DeadLetterDecision.keep()).toList();
        }).telemetry(new OutboxTelemetry() {
            @Override
            public void relayFailed(Throwable error) {
                failures.add(error);
            }
        }).build();
        closing.add(runtime);
        runtime.start();

        var sent = new ArrayList<UUID>();
        sent.addAll(transactions.inTx(c -> runtime.outbox().sendAll(List.of(
                OutboxMessage.of("slow", "s", "1".getBytes(UTF_8)), OutboxMessage.of("slow", "s", "2".getBytes(UTF_8)),
                OutboxMessage.of("slow", "s", "3".getBytes(UTF_8))))));
        var poison = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("poison", "p", new byte[1])));
        for (int tx = 0; tx < 20; tx++) {
            int n = tx;
            sent.addAll(transactions.inTx(c -> runtime.outbox().sendAll(java.util.stream.IntStream.range(0, 10)
                    .mapToObj(i -> OutboxMessage.of("fast", "k" + (n * 10 + i) % 7, new byte[] {(byte) i})).toList())));
        }

        await(Duration.ofSeconds(60), () -> effects.containsAll(sent) && hooked.contains(poison));
        Thread.sleep(2000); // maintenance runs meanwhile, and anything delivered twice would show
        runtime.relay().drain();

        assertThat(failures).as("no step failed").isEmpty();
        assertThat(calls.entrySet()).filteredOn(e -> sent.contains(e.getKey()))
                .allSatisfy(e -> assertThat(e.getValue()).as("calls of " + e.getKey()).hasValue(1));
        assertThat(effects).containsExactlyInAnyOrderElementsOf(sent);
        assertThat(hooked).containsExactly(poison);
        var status = runtime.admin().status();
        assertThat(status.halted()).isFalse();
        assertThat(status.maintenanceSinceSuccessSeconds()).as("maintenance succeeds").isLessThan(5);
        assertThat(status.instances()).singleElement().satisfies(i -> assertThat(i.live()).isTrue());
    }

    /**
     * The fleet's database turns read-only for 8 s, as a pool pointed at a standby or {@code default_transaction_read_only}
     * do, its sessions cut: writers fail, nothing is delivered from a write that failed, and once it is writable again
     * everything committed is delivered, with no halt, the heartbeat and maintenance back. A start meanwhile fails.
     */
    @Test
    void aNodeThatTurnsReadOnlyHoldsTheFleetOnlyWhileItLasts() throws Exception {
        var database = "ro_" + UUID.randomUUID().toString().substring(0, 8);
        try (var c = superuser("test")) {
            TestSql.execute(c, "create database " + database);
        }
        var ds = pool(database, Pg.INSTANCE.getUsername(), Pg.INSTANCE.getPassword(), 12);
        var transactions = new JdbcTransactions(ds);
        var config = new TestOutboxConfig().shards(4).maintenanceInterval(Duration.ofMillis(500));
        Set<UUID> delivered = ConcurrentHashMap.newKeySet();
        var runtime = OutboxRuntime.builder(config, transactions, record -> delivered.add(record.messageId())).build();
        closing.add(runtime);
        runtime.start();

        var committed = new CopyOnWriteArrayList<UUID>();
        var refused = new AtomicInteger();
        var stop = new AtomicBoolean();
        var writer = CompletableFuture.runAsync(() -> {
            for (int i = 0; !stop.get(); i++) {
                int n = i;
                try {
                    committed.add(transactions.inTx(c -> runtime.outbox()
                            .send(OutboxMessage.of("t", "k" + n % 13, Integer.toString(n).getBytes(UTF_8)))));
                } catch (RuntimeException e) {
                    refused.incrementAndGet();
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        closing.add(() -> {
            stop.set(true);
            writer.get(30, TimeUnit.SECONDS);
        });
        Thread.sleep(2000);

        readOnly(database, true);
        int before = committed.size();
        Thread.sleep(1000);
        var second = OutboxRuntime.builder(new TestOutboxConfig().table(config.table()).shards(4),
                new JdbcTransactions(ds), record -> {}).build();
        closing.add(second);
        assertThatThrownBy(second::start).as("a start on a read-only node fails, it does not hang")
                .hasStackTraceContaining("read-only");
        Thread.sleep(7000);
        assertThat(committed.size()).as("no write commits meanwhile").isEqualTo(before);
        assertThat(refused).as("writers are refused").hasPositiveValue();
        readOnly(database, false);

        await(Duration.ofSeconds(30), () -> committed.size() > before + 50);
        stop.set(true);
        writer.get(30, TimeUnit.SECONDS);
        await(Duration.ofSeconds(60), () -> delivered.containsAll(committed));
        assertThat(delivered).as("only what committed").containsExactlyInAnyOrderElementsOf(committed);
        long beaten = System.currentTimeMillis();
        await(Duration.ofSeconds(30), () -> {
            var status = runtime.admin().status();
            return !status.halted() && status.maintenanceSinceSuccessSeconds() < 2
                    && status.instances().stream().anyMatch(OutboxStatus.Instance::live);
        });
        assertThat(System.currentTimeMillis() - beaten).isLessThan(30_000);
        assertThat(runtime.relay().running()).isTrue();
    }

    /** As a promoted-then-demoted node: the setting for new sessions, every current session of the database cut. */
    private static void readOnly(String database, boolean on) throws SQLException {
        try (var c = superuser("postgres")) {
            TestSql.execute(c, "alter database " + database + " set default_transaction_read_only = " + on);
            TestSql.execute(c, "select pg_terminate_backend(pid) from pg_stat_activity where datname = '" + database
                    + "' and pid <> pg_backend_pid()");
        }
    }

    private static void await(Duration timeout, Check check) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!check.met()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("not met within " + timeout);
            }
            Thread.sleep(50);
        }
    }

    @FunctionalInterface
    private interface Check {
        boolean met() throws Exception;
    }
}
