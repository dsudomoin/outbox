package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link KafkaOutboxSender} under the relay, against a real Postgres and a real broker: each record's
 * {@code producer.send} runs in the record's scope, and each broker ack reports its record before the call returns.
 */
class KafkaRecordScopeIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private final Recording telemetry = new Recording();
    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    /** Each producer send: the message id it sends and the record whose scope was current on the sending thread. */
    private final List<Send> sends = new CopyOnWriteArrayList<>();
    private final List<AutoCloseable> closing = new ArrayList<>(); // senders and runtimes, closed last first

    @AfterEach
    void close() throws Exception {
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
    }

    @Test
    void sendRunsInTheRecordScope() throws Exception {
        var topic = TestKafka.createTopic(3);
        var runtime = start(telemetry, new Handler());
        var ids = sendAll(runtime, topic, "a", "b", "c");

        runtime.relay().drain();

        assertThat(sends).extracting(Send::messageId).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(sends).allSatisfy(s -> assertThat(s.scope()).as("the record scope of %s", s.messageId())
                .isNotNull().extracting(OutboxRecord::messageId).isEqualTo(s.messageId()));
    }

    @Test
    void eachAckEndsItsRecordBeforeTheCallReturns() throws Exception {
        var topic = TestKafka.createTopic(3);
        var handler = new Handler();
        var runtime = start(telemetry, handler);
        var ids = sendAll(runtime, topic, "a", "b", "c");

        runtime.relay().drain();
        await(() -> telemetry.finished().size() == handler.calls.get());

        var reports = telemetry.reports();
        assertThat(reports).extracting(r -> r.record().messageId()).containsExactlyInAnyOrderElementsOf(ids);
        assertThat(reports).allSatisfy(r -> {
            assertThat(r.outcome()).isEqualTo(Outcome.delivered());
            assertThat(handler.threads).as("the handler threads").contains(r.thread());
        });
        // each report came before the finished of its own call
        for (var report : reports) {
            var finished = telemetry.events.stream().filter(e -> e instanceof Finished f
                    && f.records().contains(report.record())).findFirst().orElseThrow();
            assertThat(telemetry.events.indexOf(report)).isLessThan(telemetry.events.indexOf(finished));
        }
    }

    @Test
    void aFailedSendIsNotReportedEarly() throws Exception {
        var topic = TestKafka.createTopic(3);
        var invalid = "bad/topic-" + UUID.randomUUID();
        var runtime = start(telemetry, new Handler());
        var good = sendAll(runtime, topic, "a").get(0);
        var bad = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(invalid, "k", bytes("bad"))));

        runtime.relay().drain();
        await(() -> telemetry.finished().stream().anyMatch(f -> f.indexOf(bad) >= 0));

        assertThat(telemetry.reports()).extracting(r -> r.record().messageId()).containsExactly(good);
        var finished = telemetry.finished().stream().filter(f -> f.indexOf(bad) >= 0).findFirst().orElseThrow();
        assertThat(finished.outcomes().get(finished.indexOf(bad))).isInstanceOfSatisfying(Outcome.Permanent.class,
                p -> assertThat(p.error()).isInstanceOf(InvalidTopicException.class));
    }

    @Test
    void withoutTelemetryNothingIsQueued() throws Exception {
        var topic = TestKafka.createTopic(3);
        var runtime = start(OutboxTelemetry.NONE, new Handler());
        var ids = sendAll(runtime, topic, "a", "b", "c");

        runtime.relay().drain();

        var received = TestKafka.consumeUntil(topic, TIMEOUT,
                r -> r.stream().map(KafkaOutboxHeaders::messageId).toList().containsAll(ids));
        assertThat(received).extracting(KafkaOutboxHeaders::messageId).containsAll(ids);
        assertThat(sends).extracting(Send::scope).containsOnlyNulls();
    }

    private List<UUID> sendAll(OutboxRuntime runtime, String topic, String... keys) throws Exception {
        return transactions.inTx(c -> runtime.outbox().sendAll(
                Stream.of(keys).map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));
    }

    private OutboxRuntime start(OutboxTelemetry telemetry, Handler handler) {
        var config = new TestOutboxConfig().relayEnabled(false).batchTimeout(Duration.ofSeconds(30))
                .deadLettersTimeout(Duration.ofSeconds(30));
        var runtime = OutboxRuntime.builder(config, transactions, handler).telemetry(telemetry).build();
        closing.add(runtime);
        runtime.start();
        return runtime;
    }

    /** A sender whose producers note, at each send, the record scope current on the sending thread. */
    private KafkaOutboxSender sender() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        var sender = new KafkaOutboxSender(() -> properties, props -> new KafkaProducer<byte[], byte[]>(props) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
                var id = UUID.fromString(new String(record.headers().lastHeader(KafkaOutboxHeaders.MESSAGE_ID).value(),
                        UTF_8));
                sends.add(new Send(id, telemetry.scope.get()));
                return super.send(record, callback);
            }
        });
        closing.add(sender);
        return sender;
    }

    record Send(UUID messageId, @Nullable OutboxRecord scope) {}

    /** {@link KafkaOutboxSender#handler()}, noting its calls and their threads. */
    private final class Handler implements OutboxHandler {

        private final KafkaOutboxHandler kafka = sender().handler();
        final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public void handle(OutboxRecord record) {
            kafka.handle(record);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            threads.add(Thread.currentThread());
            calls.incrementAndGet();
            return kafka.handleBatch(records);
        }
    }

    record Report(OutboxRecord record, Outcome outcome, Thread thread) {}

    record Finished(List<OutboxRecord> records, List<Outcome> outcomes) {
        int indexOf(UUID messageId) {
            for (int i = 0; i < records.size(); i++) {
                if (records.get(i).messageId().equals(messageId)) {
                    return i;
                }
            }
            return -1;
        }
    }

    /** Each delivery's reports and end, in order; its record scope is a thread-local marker. */
    private static final class Recording implements OutboxTelemetry {

        final ThreadLocal<OutboxRecord> scope = new ThreadLocal<>();
        final List<Object> events = new CopyOnWriteArrayList<>();

        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            return new Delivery() {
                @Override
                public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
                    var previous = scope.get();
                    scope.set(record);
                    try {
                        return call.call();
                    } finally {
                        scope.set(previous);
                    }
                }

                @Override
                public void recordFinished(OutboxRecord record, Outcome outcome) {
                    events.add(new Report(record, outcome, Thread.currentThread()));
                }

                @Override
                public void finished(List<Outcome> outcomes) {
                    events.add(new Finished(List.copyOf(records), List.copyOf(outcomes)));
                }
            };
        }

        List<Report> reports() {
            return events.stream().filter(Report.class::isInstance).map(Report.class::cast).toList();
        }

        List<Finished> finished() {
            return events.stream().filter(Finished.class::isInstance).map(Finished.class::cast).toList();
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("condition not met within " + TIMEOUT);
            }
            Thread.sleep(10);
        }
    }
}
