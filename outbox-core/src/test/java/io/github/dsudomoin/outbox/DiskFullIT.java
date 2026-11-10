package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The disk under Postgres fills up under load (data files and WAL on a 256 MB volume, a filler taking the rest), then
 * space comes back: writers are refused meanwhile, nothing committed is lost, and the relay goes on by itself. Fills a
 * volume and may crash the server: only with {@code -Poutbox.test.chaos}.
 */
@Tag("chaos")
class DiskFullIT {

    @Test
    void aFullDiskRefusesWritesAndLosesNothing() throws Exception {
        int port;
        try (var socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        try (var pg = new PostgreSQLContainer("postgres:16-alpine")
                .withTmpFs(Map.of("/var/lib/postgresql/data", "rw,size=256m"))
                .withCommand("postgres", "-c", "max_wal_size=64MB", "-c", "min_wal_size=32MB")
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withPortBindings(
                        new PortBinding(Ports.Binding.bindPort(port), new ExposedPort(5432))))) {
            pg.start();
            var hikari = new HikariConfig();
            hikari.setJdbcUrl(pg.getJdbcUrl());
            hikari.setUsername(pg.getUsername());
            hikari.setPassword(pg.getPassword());
            hikari.setMaximumPoolSize(10);
            hikari.setConnectionTimeout(3_000);
            hikari.addDataSourceProperty("socketTimeout", "10");
            try (var ds = new HikariDataSource(hikari)) {
                var transactions = new JdbcTransactions(ds);
                Set<UUID> delivered = ConcurrentHashMap.newKeySet();
                var config = new TestOutboxConfig().maintenanceInterval(Duration.ofMillis(500));
                try (var runtime = OutboxRuntime.builder(config, transactions,
                        record -> delivered.add(record.messageId())).build()) {
                    runtime.start();
                    // the filler leaves about 24 MB: the writers below fill that in seconds
                    exec(pg, "df -m /var/lib/postgresql/data | tail -1");
                    exec(pg, "fallocate -l $(( $(df -m /var/lib/postgresql/data | tail -1 | awk '{print $4}') - 24 ))M"
                            + " /var/lib/postgresql/data/filler || dd if=/dev/zero of=/var/lib/postgresql/data/filler"
                            + " bs=1M count=$(( $(df -m /var/lib/postgresql/data | tail -1 | awk '{print $4}') - 24 ))");
                    var committed = new CopyOnWriteArrayList<UUID>();
                    var refused = new AtomicInteger();
                    var stop = new AtomicBoolean();
                    var payload = new byte[8 * 1024];
                    var writers = CompletableFuture.allOf(java.util.stream.IntStream.range(0, 4).mapToObj(w ->
                            CompletableFuture.runAsync(() -> {
                                for (int i = 0; !stop.get(); i++) {
                                    int n = i;
                                    try {
                                        committed.addAll(transactions.inTx(c -> runtime.outbox().sendAll(
                                                java.util.stream.IntStream.range(0, 20).mapToObj(j -> OutboxMessage.of(
                                                        "t", "k" + w + "-" + (n * 20 + j) % 11, payload)).toList())));
                                    } catch (RuntimeException e) {
                                        refused.incrementAndGet();
                                        sleep(200);
                                    }
                                }
                            })).toArray(CompletableFuture[]::new));
                    long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                    while (refused.get() < 20 && System.nanoTime() < deadline) {
                        Thread.sleep(200);
                    }
                    System.out.println("disk full: " + exec(pg, "df -m /var/lib/postgresql/data | tail -1")
                            + ", committed " + committed.size() + ", refused " + refused.get());
                    assertThat(refused.get()).as("the disk filled up and writers were refused").isGreaterThan(0);
                    Thread.sleep(5000); // the relay meets the full disk too

                    exec(pg, "rm -f /var/lib/postgresql/data/filler");
                    System.out.println("space back: " + exec(pg, "df -m /var/lib/postgresql/data | tail -1"));
                    int before = committed.size();
                    deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                    while (committed.size() < before + 200 && System.nanoTime() < deadline) {
                        Thread.sleep(200);
                    }
                    stop.set(true);
                    writers.get(60, TimeUnit.SECONDS);
                    assertThat(committed.size()).as("writes commit again").isGreaterThanOrEqualTo(before + 200);
                    deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
                    while (!delivered.containsAll(committed) && System.nanoTime() < deadline) {
                        Thread.sleep(200);
                    }
                    var lost = committed.stream().filter(id -> !delivered.contains(id)).count();
                    System.out.println("committed " + committed.size() + ", delivered " + delivered.size() + ", lost "
                            + lost);
                    assertThat(lost).as("nothing committed is lost").isZero();
                    assertThat(delivered).as("nothing that was refused arrives")
                            .containsExactlyInAnyOrderElementsOf(committed);
                    assertThat(runtime.relay().running()).isTrue();
                    deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                    while (runtime.admin().status().maintenanceSinceSuccessSeconds() > 2
                            && System.nanoTime() < deadline) {
                        Thread.sleep(200);
                    }
                    assertThat(runtime.admin().status().maintenanceSinceSuccessSeconds()).as("maintenance is back")
                            .isLessThanOrEqualTo(2);
                }
            }
        }
    }

    private static String exec(PostgreSQLContainer pg, String command) throws Exception {
        var result = pg.execInContainer("sh", "-c", command);
        return (result.getStdout() + result.getStderr()).trim();
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
