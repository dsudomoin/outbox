package io.github.dsudomoin.outbox.kafka.kora;

import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.ConfigFactory;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxConfig;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxSender;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.config.common.extractor.PropertiesConfigValueExtractor;
import ru.tinkoff.kora.config.common.origin.SimpleConfigOrigin;
import ru.tinkoff.kora.config.hocon.HoconConfigFactory;

/**
 * The warnings {@link KafkaOutboxModule} gives when a Kafka call can outlast the caller's own time limit, and the
 * producer settings of its sender.
 */
class KafkaOutboxModuleTest {

    private static final String TEMPLATE = "{topic}.dlq";

    /**
     * A call can take delivery.timeout.ms + 5 s + max.block.ms; the relay gives it batchTimeout. The defaults fit:
     * 20 s + 5 s + 5 s against 30 s.
     */
    @Test
    void warnsOnlyWhenACallCanOutlastBatchTimeout() throws Exception {
        KafkaOutboxConfig defaults = () -> props("bootstrap.servers", "localhost:9092");
        KafkaOutboxConfig slowMetadata = () -> props("bootstrap.servers", "localhost:9092", "max.block.ms", 10000);
        var module = new KafkaOutboxModule() {};
        var relayDefault = outboxConfig(new OutboxConfig.RelayConfig() {}.batchTimeout());
        try (var err = new CapturedStderr()) {
            module.kafkaOutboxSender(defaults, relayDefault).release();
            module.kafkaOutboxSender(slowMetadata, outboxConfig(Duration.ofSeconds(35))).release();
            module.kafkaOutboxSender(slowMetadata, null).release(); // no relay in the app
            assertThat(err.text()).doesNotContain("batchTimeout");
        }
        try (var err = new CapturedStderr()) {
            module.kafkaOutboxSender(slowMetadata, relayDefault).release();
            assertThat(err.text()).contains("WARN").contains("batchTimeout");
        }
    }

    /**
     * A sender's call can take delivery.timeout.ms + 5 s + max.block.ms; the dead-letter pass gives a hook call
     * {@code outbox.deadLetters.timeout}. The defaults fit: 20 s + 5 s + 5 s against 30 s.
     */
    @Test
    void warnsOnlyWhenACallCanOutlastTheHookTimeout() {
        var module = new KafkaOutboxModule() {};
        var outbox = new OutboxConfig() {};
        var defaults = kafkaConfig(TEMPLATE, props("bootstrap.servers", "dlq-test:9092"));
        var slow = kafkaConfig(TEMPLATE, props("bootstrap.servers", "dlq-test:9092", "max.block.ms", "10000"));
        var off = kafkaConfig(null, props("bootstrap.servers", "dlq-test:9092", "max.block.ms", "10000"));
        try (var err = new CapturedStderr()) {
            module.kafkaDeadLetterHandler(defaults, new KafkaOutboxSender(defaults), outbox);
            module.kafkaDeadLetterHandler(slow, new KafkaOutboxSender(slow), null); // no relay in the app
            module.kafkaDeadLetterHandler(off, new KafkaOutboxSender(off), outbox); // no calls
            assertThat(err.text()).doesNotContain("deadLetters.timeout");
        }
        try (var err = new CapturedStderr()) {
            module.kafkaDeadLetterHandler(slow, new KafkaOutboxSender(slow), outbox);
            assertThat(err.text()).contains("WARN").contains("outbox.deadLetters.timeout");
        }
    }

    /**
     * The module's sender builds its producers with what {@link KafkaOutboxSender} forces and defaults, as the
     * producer's own config log shows it: the module passes {@code driverProperties} on and adds nothing.
     */
    @Test
    void theSendersProducersGetTheForcedSettings() throws Exception {
        KafkaOutboxConfig config = () -> props("bootstrap.servers", "localhost:9092",
                "key.serializer", StringSerializer.class.getName());
        var sender = new KafkaOutboxModule() {}.kafkaOutboxSender(config, null);
        var record = new OutboxRecord(1, UUID.randomUUID(), "t", null, new byte[0], List.of(), Instant.now(), 0, 0);
        try (var err = new CapturedStderr()) {
            Thread.currentThread().interrupt(); // the call builds its producer, then stops before sending: no broker
            assertThat(sender.value().send(List.of(record))).containsExactly(Outcome.notAttempted());
            assertThat(Thread.interrupted()).isTrue();
            assertThat(err.text()).contains("ProducerConfig values", "acks = -1", "enable.idempotence = true",
                    "delivery.timeout.ms = 20000", "request.timeout.ms = 15000", "max.block.ms = 5000",
                    "key.serializer = class " + ByteArraySerializer.class.getName());
        } finally {
            sender.release();
        }
    }

    /** {@code outbox.kafka.clientIdSuffix} reads as the core's default, true, and false when set so. */
    @Test
    void clientIdSuffixReadsFromHocon() {
        var module = new KafkaOutboxModule() {};
        var extractor = new $KoraKafkaOutboxConfig_ConfigValueExtractor(new PropertiesConfigValueExtractor());
        var on = HoconConfigFactory.fromHocon(new SimpleConfigOrigin("test"),
                ConfigFactory.parseString("outbox.kafka.driverProperties { \"bootstrap.servers\" = \"a:1\" }"));
        var off = HoconConfigFactory.fromHocon(new SimpleConfigOrigin("test"),
                ConfigFactory.parseString("outbox.kafka { driverProperties {}, clientIdSuffix = false }"));
        assertThat(module.kafkaOutboxConfig(on, extractor).clientIdSuffix()).isTrue();
        assertThat(module.kafkaOutboxConfig(off, extractor).clientIdSuffix()).isFalse();
    }

    private static OutboxConfig outboxConfig(Duration batchTimeout) {
        return new OutboxConfig() {
            @Override
            public RelayConfig relay() {
                return new RelayConfig() {
                    @Override
                    public Duration batchTimeout() {
                        return batchTimeout;
                    }
                };
            }
        };
    }

    private static KafkaOutboxConfig kafkaConfig(@Nullable String template, Properties driverProperties) {
        return new KafkaOutboxConfig() {
            @Override
            public Properties driverProperties() {
                return driverProperties;
            }

            @Override
            public String deadLetterTopic() {
                return template;
            }
        };
    }

    private static Properties props(Object... keyValues) {
        var p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            p.put(keyValues[i], keyValues[i + 1]);
        }
        return p;
    }
}
