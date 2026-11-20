package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxSenderTest.TestProducer;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InterruptException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link KafkaOutboxSender} inside the relay's handler call, in real time, with the default timeouts
 * ({@code delivery.timeout.ms} 20 s, {@code max.block.ms} 5 s, {@code batchTimeout} 30 s): {@link OutboxRuntime} over
 * Postgres, and a stub producer in place of Kafka.
 */
class KafkaOutboxSenderInRelayIT {

    private static final Duration BATCH_TIMEOUT = Duration.ofSeconds(30);
    /** A broker restart: the topic's metadata arrives late. */
    private static final Duration FIRST_LOOKUP = Duration.ofMillis(4500);
    /** Inside {@code delivery.timeout.ms}, so Kafka answers; past the relay's soft deadline (24 s) from the lookup. */
    private static final Duration ACK_AFTER = Duration.ofMillis(19_800);

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final ScheduledExecutorService broker = Executors.newSingleThreadScheduledExecutor();
    private final List<AutoCloseable> closing = new ArrayList<>(); // closed last first

    @AfterEach
    void close() throws Exception {
        for (int i = closing.size() - 1; i >= 0; i--) {
            closing.get(i).close();
        }
        broker.shutdownNow();
    }

    /**
     * The records are handed over at 4.5 s and Kafka acknowledges them at 24.3 s, after the relay's soft deadline:
     * the wind-down must not close the producer, or the records go again through a new producer, whose new producer
     * id the broker cannot tell from another writer's: a duplicate in the topic.
     */
    @Test
    void recordsHandedOverLateAreAwaitedThroughTheWindDownAndSentOnce() throws Exception {
        var handed = new ConcurrentHashMap<UUID, AtomicInteger>();
        var acked = ConcurrentHashMap.<UUID>newKeySet();
        var slowLookup = new AtomicBoolean(true);
        var sender = new KafkaOutboxSender(() -> props(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"),
                properties -> new SlowBroker(slowLookup, handed, acked));
        closing.add(sender);
        var config = new TestOutboxConfig().shards(1).batchTimeout(BATCH_TIMEOUT);
        var runtime = OutboxRuntime.builder(config, transactions, sender.handler()).build();
        closing.add(runtime);
        runtime.start();

        var ids = transactions.inTx(c -> runtime.outbox().sendAll(List.of(
                OutboxMessage.of(KafkaOutboxSenderTest.TOPIC, "a", bytes("a")),
                OutboxMessage.of(KafkaOutboxSenderTest.TOPIC, "b", bytes("b")),
                OutboxMessage.of(KafkaOutboxSenderTest.TOPIC, "c", bytes("c")))));

        long deadline = System.nanoTime() + BATCH_TIMEOUT.multipliedBy(2).toNanos();
        while (!acked.containsAll(ids) && handed.values().stream().allMatch(n -> n.get() == 1)
                && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(handed).as("each message handed to send() once").containsOnlyKeys(ids)
                .allSatisfy((id, sends) -> assertThat(sends).hasValue(1));
        assertThat(acked).as("delivered").containsExactlyInAnyOrderElementsOf(ids);
    }

    /**
     * Acknowledges each record {@link #ACK_AFTER} after {@code send()} returned; the first metadata lookup of the test
     * takes {@link #FIRST_LOOKUP}; {@code close(Duration.ZERO)} fails what is pending, as Kafka's forced close does.
     */
    private final class SlowBroker extends TestProducer {
        private final AtomicBoolean slowLookup;
        private final Map<UUID, AtomicInteger> handed;
        private final Set<UUID> acked;

        SlowBroker(AtomicBoolean slowLookup, Map<UUID, AtomicInteger> handed, Set<UUID> acked) {
            super(false, r -> null);
            this.slowLookup = slowLookup;
            this.handed = handed;
            this.acked = acked;
        }

        @Override
        public List<PartitionInfo> partitionsFor(String topic) {
            if (slowLookup.getAndSet(false)) {
                try {
                    Thread.sleep(FIRST_LOOKUP.toMillis());
                } catch (InterruptedException e) {
                    throw new InterruptException(e);
                }
            }
            return super.partitionsFor(topic);
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, Callback callback) {
            var id = UUID.fromString(new String(record.headers().lastHeader(KafkaOutboxHeaders.MESSAGE_ID).value(),
                    UTF_8));
            handed.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
            var sent = super.send(record, (metadata, e) -> {
                if (e == null) {
                    acked.add(id);
                }
                callback.onCompletion(metadata, e);
            });
            broker.schedule(() -> completeNext(), ACK_AFTER.toMillis(), TimeUnit.MILLISECONDS);
            return sent;
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static Properties props(String key, String value) {
        var p = new Properties();
        p.put(key, value);
        return p;
    }
}
