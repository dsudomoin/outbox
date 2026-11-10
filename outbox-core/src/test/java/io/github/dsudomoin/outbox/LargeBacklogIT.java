package io.github.dsudomoin.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * A backlog of medium-sized messages, as a target's outage leaves one, met by a service with a usual heap and the
 * library's defaults (4 workers, batches of 1000): the relay must work it off, not run out of memory on it. 400 MB
 * written and a JVM of its own: only with {@code -Poutbox.test.chaos}.
 */
@Tag("chaos")
class LargeBacklogIT {

    private static final int MESSAGES = 4000;
    private static final int PAYLOAD = 100 * 1024;

    /** Default settings but the table; a fresh outbox, so 256 shards. */
    private static OutboxConfig defaults(String table) {
        return new OutboxConfig() {
            @Override
            public String table() {
                return table;
            }
        };
    }

    @Test
    void aBacklogOf100KbMessagesIsDeliveredByA512MbService() throws Exception {
        var table = TestSql.uniqueTable();
        var transactions = new JdbcTransactions(TestDataSources.newDataSource());
        try (var writer = OutboxRuntime.builder(new OutboxConfig() {
            @Override
            public String table() {
                return table;
            }

            @Override
            public RelayConfig relay() {
                return new RelayConfig() {
                    @Override
                    public boolean enabled() {
                        return false;
                    }
                };
            }
        }, transactions, record -> {}).build()) {
            writer.start();
            var random = new Random(7);
            for (int tx = 0; tx < MESSAGES / 100; tx++) {
                var messages = new ArrayList<OutboxMessage>();
                for (int i = 0; i < 100; i++) {
                    var payload = new byte[PAYLOAD];
                    random.nextBytes(payload); // incompressible, as most encrypted or encoded payloads are
                    messages.add(OutboxMessage.of("docs", "k" + (tx * 100 + i), payload));
                }
                transactions.inTx(c -> writer.outbox().sendAll(messages));
            }
        }

        var log = File.createTempFile("large-backlog", ".log");
        var java = ProcessHandle.current().info().command().orElse("java");
        var child = new ProcessBuilder(java, "-Xmx512m", "-cp", System.getProperty("java.class.path"),
                Child.class.getName(), TestDataSources.jdbcUrl(), TestDataSources.user(), TestDataSources.password(),
                table, Integer.toString(MESSAGES))
                .redirectErrorStream(true).redirectOutput(log).start();
        boolean exited = child.waitFor(150, TimeUnit.SECONDS);
        if (!exited) {
            child.destroyForcibly().waitFor();
        }
        var output = Files.readString(log.toPath());
        var lines = output.lines().toList();
        System.out.println("child output, last lines:\n" + String.join("\n",
                lines.subList(Math.max(0, lines.size() - 40), lines.size())));

        assertThat(output).as("the relay never runs out of memory").doesNotContain("OutOfMemoryError");
        assertThat(exited).as("delivered all within 150 s").isTrue();
        assertThat(child.exitValue()).isZero();
        assertThat(output).contains("delivered " + MESSAGES + " of " + MESSAGES);
    }

    /** The service: the library's defaults, a handler that keeps nothing. Exits 0 once all arrived. */
    public static final class Child {

        public static void main(String[] args) throws Exception {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                System.out.println("uncaught on " + t.getName() + ": " + e); // the JVM's default: the thread ends
            });
            var ds = new PGSimpleDataSource();
            ds.setUrl(args[0]);
            ds.setUser(args[1]);
            ds.setPassword(args[2]);
            int expected = Integer.parseInt(args[4]);
            var delivered = new AtomicLong();
            var failures = new AtomicLong();
            try (var runtime = OutboxRuntime.builder(defaults(args[3]), new JdbcTransactions(ds),
                    record -> delivered.incrementAndGet()).telemetry(new OutboxTelemetry() {
                        @Override
                        public void relayFailed(Throwable error) {
                            if (failures.incrementAndGet() <= 5) {
                                System.out.println("relay failed: " + error);
                            }
                            if (error instanceof OutOfMemoryError || error.getCause() instanceof OutOfMemoryError) {
                                System.out.println("OutOfMemoryError in the relay");
                            }
                        }
                    }).build()) {
                runtime.start();
                long deadline = System.nanoTime() + Duration.ofSeconds(140).toNanos();
                long last = -1;
                long since = System.nanoTime();
                boolean dumped = false;
                while (delivered.get() < expected && System.nanoTime() < deadline) {
                    Thread.sleep(500);
                    if (delivered.get() != last) {
                        last = delivered.get();
                        since = System.nanoTime();
                    } else if (!dumped && System.nanoTime() - since > Duration.ofSeconds(20).toNanos()) {
                        dumped = true; // stalled: what the relay threads and their sessions do
                        Thread.getAllStackTraces().forEach((t, stack) -> {
                            if (t.getName().startsWith("outbox")) {
                                System.out.println("STALL thread " + t.getName() + " " + t.getState());
                                for (int i = 0; i < Math.min(12, stack.length); i++) {
                                    System.out.println("STALL   at " + stack[i]);
                                }
                            }
                        });
                        try (var c = ds.getConnection(); var st = c.createStatement(); var rs = st.executeQuery(
                                "select pid, state, wait_event_type, wait_event, now() - xact_start, left(query, 120)"
                                        + " from pg_stat_activity where datname = current_database()"
                                        + " and pid <> pg_backend_pid()")) {
                            while (rs.next()) {
                                System.out.println("STALL session " + rs.getString(1) + " " + rs.getString(2) + " "
                                        + rs.getString(3) + "/" + rs.getString(4) + " xact " + rs.getString(5) + ": "
                                        + rs.getString(6));
                            }
                        }
                        try (var c = ds.getConnection(); var st = c.createStatement(); var rs = st.executeQuery(
                                "select count(*), count(*) filter (where paused_until > now()), max(failures)"
                                        + " from " + args[3] + "_cursor")) {
                            rs.next();
                            System.out.println("STALL cursors " + rs.getString(1) + " paused " + rs.getString(2)
                                    + " max failures " + rs.getString(3));
                        } catch (Exception e) {
                            System.out.println("STALL cursors unreadable: " + e);
                        }
                    }
                    var heap = Runtime.getRuntime();
                    System.out.println("delivered " + delivered.get() + ", heap used "
                            + (heap.totalMemory() - heap.freeMemory()) / (1 << 20) + " MB, relay failures "
                            + failures.get());
                }
                System.out.println("delivered " + delivered.get() + " of " + expected);
            }
            System.exit(delivered.get() >= expected ? 0 : 2);
        }
    }
}
