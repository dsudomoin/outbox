package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.net.ServerSocket;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Writes the usual tests do not make: a send in a prepared (two-phase) transaction left across a Postgres restart, a
 * writer with more savepoints than a backend caches subtransactions for, payloads of every byte and of 10 MB, a topic
 * Postgres cannot store, and 100 000 messages in one transaction.
 */
class UnusualWritesIT {

    private final List<AutoCloseable> closing = new ArrayList<>();
    private final Map<UUID, byte[]> delivered = new ConcurrentHashMap<>();
    private final List<String> order = new CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicInteger duplicates = new java.util.concurrent.atomic.AtomicInteger();

    @AfterEach
    void close() throws Exception {
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
    }

    private OutboxRuntime start(OutboxConfig config, OutboxTransactions transactions) {
        var runtime = OutboxRuntime.builder(config, transactions, record -> {
            if (delivered.put(record.messageId(), record.payload() == null ? new byte[0] : record.payload()) != null) {
                duplicates.incrementAndGet();
            }
            order.add(record.keyAsString() + "/" + new String(record.payload(), UTF_8));
        }).build();
        closing.add(runtime);
        runtime.start();
        return runtime;
    }

    /**
     * A send prepared for commit by a transaction manager (XA, {@code PREPARE TRANSACTION}) is neither delivered nor
     * lost while it waits, a restart in between: committed later, it is delivered; rolled back, never. Others flow on.
     */
    @Test
    void aPreparedSendSurvivesARestartAndIsDeliveredOnlyIfCommitted() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        var pg = new PostgreSQLContainer("postgres:16-alpine")
                .withCommand("postgres", "-c", "fsync=off", "-c", "max_prepared_transactions=10")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(5432))));
        pg.start();
        closing.add(pg);
        var hikari = new HikariConfig();
        hikari.setJdbcUrl(pg.getJdbcUrl());
        hikari.setUsername(pg.getUsername());
        hikari.setPassword(pg.getPassword());
        hikari.setMaximumPoolSize(10);
        hikari.setConnectionTimeout(3_000);
        hikari.addDataSourceProperty("socketTimeout", "10");
        var ds = new HikariDataSource(hikari);
        closing.add(ds);
        var transactions = new JdbcTransactions(ds);
        var runtime = start(new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(500)), transactions);
        var outbox = runtime.outbox();

        var commit = prepare(transactions, runtime, "gx-commit", "p", "commit");
        var rollback = prepare(transactions, runtime, "gx-rollback", "r", "rollback");
        // the prepared transaction keeps its key's lock, as any open one does: a send of that key waits for it
        assertThatThrownBy(() -> transactions.inTx(c -> {
            TestSql.execute(c, "set local lock_timeout = '1s'");
            return outbox.send(OutboxMessage.of("t", "p", bytes("blocked")));
        })).hasStackTraceContaining("lock timeout");
        var before = transactions.inTx(c -> outbox.send(OutboxMessage.of("t", "f", bytes("before"))));
        await(Duration.ofSeconds(10), () -> delivered.containsKey(before));

        String started;
        try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
            started = TestSql.queryString(c, "select pg_postmaster_start_time()::text");
        }
        pg.getDockerClient().restartContainerCmd(pg.getContainerId()).withTimeout(10).exec();
        UUID after = null;
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (after == null) {
            try {
                after = transactions.inTx(c -> outbox.send(OutboxMessage.of("t", "f", bytes("after"))));
            } catch (RuntimeException e) {
                assertThat(System.nanoTime()).as("Postgres back within 60 s").isLessThan(deadline);
                Thread.sleep(500);
            }
        }
        UUID back = after;
        await(Duration.ofSeconds(30), () -> delivered.containsKey(back));
        try (var c = DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword())) {
            assertThat(TestSql.queryString(c, "select pg_postmaster_start_time()::text")).as("restarted")
                    .isNotEqualTo(started);
            assertThat(TestSql.queryString(c, "select string_agg(gid, ',' order by gid) from pg_prepared_xacts"))
                    .as("both still prepared after the restart").isEqualTo("gx-commit,gx-rollback");
            assertThat(delivered).doesNotContainKeys(commit, rollback);
            TestSql.execute(c, "commit prepared 'gx-commit'");
            TestSql.execute(c, "rollback prepared 'gx-rollback'");
        }

        await(Duration.ofSeconds(30), () -> delivered.containsKey(commit));
        var last = transactions.inTx(c -> outbox.send(OutboxMessage.of("t", "p", bytes("last"))));
        await(Duration.ofSeconds(10), () -> delivered.containsKey(last));
        runtime.relay().drain();
        assertThat(delivered).doesNotContainKey(rollback);
        assertThat(order).containsSubsequence("p/commit", "p/last");
        assertThat(order).doesNotContain("p/blocked", "r/rollback");
    }

    /**
     * Begin, send, {@code PREPARE TRANSACTION}, as a transaction manager's first phase; the session is free again after,
     * and pgjdbc's commit is a no-op once the server reports no transaction.
     */
    private static UUID prepare(JdbcTransactions transactions, OutboxRuntime runtime, String gid, String key,
            String payload) {
        return transactions.inTx(c -> {
            var id = runtime.outbox().send(OutboxMessage.of("t", key, bytes(payload)));
            TestSql.execute(c, "prepare transaction '" + gid + "'");
            return id;
        });
    }

    /**
     * 100 savepoints in one writer transaction, every third rolled back, held open while the relay ticks: only the
     * released ones arrive, after the commit, in order.
     */
    @Test
    void sendsInRolledBackSavepointsNeverArrive() throws Exception {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var runtime = start(new TestOutboxConfig(), transactions);
        var kept = new ArrayList<String>();
        transactions.inTx(c -> {
            for (int i = 0; i < 100; i++) {
                var savepoint = c.setSavepoint();
                runtime.outbox().send(OutboxMessage.of("t", "s", bytes(Integer.toString(i))));
                if (i % 3 == 0) {
                    c.rollback(savepoint);
                } else {
                    c.releaseSavepoint(savepoint);
                    kept.add("s/" + i);
                }
            }
            try {
                Thread.sleep(1000); // ticks pass over the open transaction
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            assertThat(order).as("nothing before the commit").isEmpty();
            return null;
        });
        await(Duration.ofSeconds(15), () -> order.size() >= kept.size());
        runtime.relay().drain();
        assertThat(order).containsExactlyElementsOf(kept);
        assertThat(duplicates).hasValue(0);
    }

    /** Payload, key and headers carry any bytes, NUL and invalid UTF-8 included, and 10 MB arrive whole. */
    @Test
    void anyBytesAndTenMegabytesArriveExactly() throws Exception {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var received = new ConcurrentHashMap<UUID, OutboxRecord>();
        var runtime = OutboxRuntime.builder(new TestOutboxConfig(), transactions,
                record -> received.put(record.messageId(), record)).build();
        closing.add(runtime);
        runtime.start();
        var odd = new byte[] {0, (byte) 0xC3, 0x28, (byte) 0xFF, (byte) 0xFE, 0, 'a', (byte) 0x80};
        var big = new byte[10 * 1024 * 1024];
        new Random(1).nextBytes(big);
        var oddId = transactions.inTx(c -> runtime.outbox().send(
                OutboxMessage.of("t", odd, odd).withHeader("h\u0000", new byte[] {0, (byte) 0xFF})));
        var bigId = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of("t", "big", big)));

        await(Duration.ofSeconds(30), () -> received.containsKey(oddId) && received.containsKey(bigId));
        assertThat(received.get(oddId).key()).isEqualTo(odd);
        assertThat(received.get(oddId).payload()).isEqualTo(odd);
        assertThat(received.get(oddId).headers()).singleElement().satisfies(h -> {
            assertThat(h.name()).isEqualTo("h\u0000");
            assertThat(h.value()).isEqualTo(new byte[] {0, (byte) 0xFF});
        });
        assertThat(Arrays.equals(received.get(bigId).payload(), big)).isTrue();
    }

    /**
     * A topic with NUL, which Postgres text cannot hold, is refused before any SQL: the caller's transaction stays
     * usable and commits the next send.
     */
    @Test
    void aTopicPostgresCannotStoreFailsTheSend() {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var runtime = start(new TestOutboxConfig(), transactions);

        var ok = transactions.inTx(c -> {
            assertThatThrownBy(() -> runtime.outbox().send(OutboxMessage.of("t\u0000x", "k", bytes("nul"))))
                    .isInstanceOf(IllegalArgumentException.class);
            TestSql.execute(c, "select 1");
            return runtime.outbox().send(OutboxMessage.of("t", "k", bytes("ok")));
        });

        assertThat(order).isEmpty();
        assertThatAwait(() -> delivered.containsKey(ok));
    }

    /** 100 000 messages in one transaction: delivered, each once. */
    @Test
    void aHundredThousandInOneTransaction() throws Exception {
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        var runtime = start(new TestOutboxConfig().batchSize(1000), transactions);
        var messages = IntStream.range(0, 100_000)
                .mapToObj(i -> OutboxMessage.of("t", "k" + i % 1000, bytes(Integer.toString(i)))).toList();
        long t0 = System.nanoTime();
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(messages));
        long wrote = System.nanoTime() - t0;

        await(Duration.ofSeconds(120), () -> delivered.size() >= ids.size());
        System.out.printf("100 000 in one transaction: written in %d ms, delivered after %d ms%n", wrote / 1_000_000,
                (System.nanoTime() - t0) / 1_000_000);
        assertThat(delivered.keySet()).isEqualTo(new java.util.HashSet<>(ids)); // a set: 100 000 compared at once
        assertThat(duplicates).hasValue(0);
    }

    private static void assertThatAwait(Check check) {
        try {
            await(Duration.ofSeconds(10), check);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
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
