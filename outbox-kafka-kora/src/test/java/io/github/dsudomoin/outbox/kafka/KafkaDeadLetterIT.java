package io.github.dsudomoin.outbox.kafka;

import static io.github.dsudomoin.outbox.testing.TestSql.queryLong;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRelay;
import io.github.dsudomoin.outbox.internal.DeadLetterPass;
import io.github.dsudomoin.outbox.internal.Maintenance;
import io.github.dsudomoin.outbox.internal.OutboxSchema;
import io.github.dsudomoin.outbox.internal.PgOutbox;
import io.github.dsudomoin.outbox.kafka.kora.KafkaOutboxModule;
import io.github.dsudomoin.outbox.kora.KoraOutboxTelemetry;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.TestDatabase;
import io.github.dsudomoin.outbox.testing.TestDb;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.database.jdbc.JdbcHelper.SqlFunction0;

/** The relay and the module's dead-letter hook over one {@link KafkaOutboxSender}, against a real broker. */
class KafkaDeadLetterIT {

    private final TestDatabase db = TestDb.newDatabase(10);
    // the sender bounds a call by delivery.timeout.ms (20 s) + 5 s: the relay and the hook must give it longer
    private final TestOutboxConfig config = new TestOutboxConfig()
            .batchTimeout(Duration.ofSeconds(30))
            .deadLettersTimeout(Duration.ofSeconds(30));
    private final OutboxMetrics metrics = new OutboxMetrics(null);
    private KafkaOutboxSender sender;
    private OutboxSchema schema;
    private OutboxRelay relay;
    private PgOutbox outbox;
    private DeadLetterPass pass;

    @AfterEach
    void release() {
        if (relay != null) {
            relay.close();
        }
        if (pass != null) {
            pass.close();
        }
        if (sender != null) {
            sender.close();
        }
        db.release();
        Context.clear();
    }

    /**
     * The record is larger than its topic's {@code max.message.bytes}, so the broker refuses it for good and it dies
     * on its first attempt; its dead-letter topic has Kafka's default limit and takes it.
     */
    @Test
    void deadLetterIsPublishedToItsTopicAndDiscarded() throws Exception {
        var topic = TestKafka.createTopic("t-" + UUID.randomUUID(), 1, Map.of("max.message.bytes", "1024"));
        var dlq = TestKafka.createTopic(topic + ".dlq", 1, Map.of());
        start("{topic}.dlq");
        var payload = new byte[10 * 1024];

        var id = db.inTx((SqlFunction0<UUID>) () -> outbox.send(
                OutboxMessage.of(topic, "k", payload).withHeader("h", bytes("a"))));

        var record = TestKafka.consumeUntil(dlq, Duration.ofSeconds(30), r -> !r.isEmpty()).get(0);
        assertThat(record.key()).isEqualTo(bytes("k"));
        assertThat(record.value()).isEqualTo(payload);
        assertThat(record.headers().toArray()).extracting(Header::key).containsExactly("h",
                KafkaDeadLetterHandler.DEAD_TOPIC, KafkaDeadLetterHandler.DEAD_ERROR,
                KafkaDeadLetterHandler.DEAD_ATTEMPTS, KafkaDeadLetterHandler.DEAD_FAILED_AT,
                KafkaOutboxHeaders.MESSAGE_ID);
        assertThat(header(record.headers().lastHeader("h"))).isEqualTo("a");
        assertThat(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_TOPIC))).isEqualTo(topic);
        assertThat(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_ERROR)))
                .contains("RecordTooLargeException");
        assertThat(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_ATTEMPTS))).isEqualTo("1");
        assertThat(Instant.parse(header(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_FAILED_AT))))
                .isBefore(Instant.now());
        assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(id);

        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        long deadRows = deadRows(id);
        while (deadRows > 0 && System.nanoTime() < deadline) { // the discard commits after the publish
            Thread.sleep(20);
            deadRows = deadRows(id);
        }
        assertThat(deadRows).as("the letter was discarded").isZero();
    }

    /**
     * The broker creates no topics on first use and one letter's dead-letter topic is missing: that letter alone is
     * handed again, its batch-mates are on their dead-letter topic once and discarded in the call that published them.
     * The first call waits out the missing topic's metadata and tries nothing else (one such wait per call), the
     * second publishes the batch-mates, the third has only the letter left.
     */
    @Test
    void aMissingDeadLetterTopicDoesNotHoldItsBatchMates() throws Exception {
        var topic = "t-" + UUID.randomUUID();
        var dlq = TestKafka.createTopic(topic + ".dlq", 1, Map.of());
        config.retry(Duration.ofMillis(50), Duration.ofMillis(200), 0);
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "2000"); // the metadata wait for the missing topic
        sender = new KafkaOutboxSender(() -> properties);
        schema = new OutboxSchema(db, config);
        schema.start();
        pass = new DeadLetterPass(db, schema.names(), config, new KafkaDeadLetterHandler(sender, "{topic}.dlq"),
                metrics);
        long waiting = deadLetter(1, "x-" + UUID.randomUUID());
        var mates = List.of(deadLetter(2, topic), deadLetter(3, topic), deadLetter(4, topic));

        for (int run = 0; run < 3; run++) {
            pass.runOnce();
            execute("update " + schema.names().dead() + " set due_at = now()");
        }

        assertThat(endOffset(dlq)).as("each batch-mate once").isEqualTo(3);
        for (long id : mates) {
            assertThat(column(id, "status")).as("discarded").isNull();
        }
        assertThat(column(waiting, "status")).isEqualTo("pending");
        assertThat(column(waiting, "handler_attempts")).isEqualTo("3");
    }

    /** A pending dead letter of {@code topic} as the relay writes it, due now. */
    private long deadLetter(long id, String topic) throws Exception {
        execute("insert into " + schema.names().dead() + " (id, shard, message_id, topic, key, payload, headers,"
                + " created_at, attempts, error, status, due_at) values (" + id + ", 0, gen_random_uuid(), '" + topic
                + "', 'k'::bytea, 'v'::bytea, null, now(), 3, 'boom', 'pending', now())");
        return id;
    }

    @Nullable
    private String column(long id, String column) throws Exception {
        try (var c = TestDb.connect(); var st = c.createStatement();
                var rs = st.executeQuery("select " + column + "::text from " + schema.names().dead() + " where id = "
                        + id)) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private void execute(String sql) throws Exception {
        try (var c = TestDb.connect()) {
            TestSql.execute(c, sql);
        }
    }

    private static long endOffset(String topic) throws Exception {
        var partition = new TopicPartition(topic, 0);
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                TestKafka.bootstrapServers()))) {
            return admin.listOffsets(Map.of(partition, OffsetSpec.latest())).all().get().get(partition).offset();
        }
    }

    /** Sender and hook as {@link KafkaOutboxModule} makes them. */
    private void start(String deadLetterTopic) throws Exception {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        var kafka = new KafkaOutboxConfig() {
            @Override
            public Properties driverProperties() {
                return properties;
            }

            @Override
            public String deadLetterTopic() {
                return deadLetterTopic;
            }
        };
        var module = new KafkaOutboxModule() {};
        sender = module.kafkaOutboxSender(kafka, config).value();
        var hook = module.kafkaDeadLetterHandler(kafka, sender, config);
        schema = new OutboxSchema(db, config);
        schema.start();
        relay = new OutboxRelay(db, config, schema, sender.handler(), new KoraOutboxTelemetry(metrics, null),
                new Maintenance(db, schema.names(), config, metrics),
                new DeadLetterPass(db, schema.names(), config, hook, metrics));
        relay.start();
        outbox = new PgOutbox(db, schema, config.shards(), relay::wake, new KoraOutboxTelemetry(metrics, null));
    }

    private long deadRows(UUID messageId) throws Exception {
        try (var c = TestDb.connect()) {
            return queryLong(c, "select count(*) from " + schema.names().dead() + " where message_id = '" + messageId
                    + "'");
        }
    }

    private static String header(Header header) {
        return new String(header.value(), UTF_8);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }
}
