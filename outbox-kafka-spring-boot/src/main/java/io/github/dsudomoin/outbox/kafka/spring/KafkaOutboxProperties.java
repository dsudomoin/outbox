package io.github.dsudomoin.outbox.kafka.spring;

import jakarta.annotation.Nullable;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The Kafka relay's settings under {@code outbox.kafka.*} (kebab-case canonical, camelCase binds too). To reuse Spring
 * Kafka's servers, write a placeholder:
 * {@code outbox.kafka.driver-properties.bootstrap.servers=${spring.kafka.bootstrap-servers}}.
 *
 * @param driverProperties Kafka producer properties; bootstrap.servers is required once anything sends to Kafka. Keys
 *     bind with their dots, with or without brackets: outbox.kafka.driver-properties.max.block.ms=3000. The sender
 *     forces byte-array serializers, enable.idempotence=true and acks=all.
 * @param deadLetterTopic where dead letters are published, {topic} standing for the letter's topic, e.g. {topic}.dlq.
 *     Not set (the default): dead letters stay in the dead table.
 * @param fallback true: Kafka takes every topic no OutboxTopicHandler claims, and an application's own OutboxHandler
 *     bean fails the start. false: no Kafka fallback; the application's OutboxHandler takes those topics or, with none,
 *     a send to one of them is refused. Default true.
 * @param clientIdSuffix true: a set client.id gets a suffix per pooled producer, <client.id>-outbox-<n> and
 *     <client.id>-outbox-unbatched-<n>, so each has its own JMX metrics. false: every producer gets client.id as set,
 *     and their JMX metrics collide. Default true.
 */
@ConfigurationProperties("outbox.kafka")
public record KafkaOutboxProperties(
    Map<String, String> driverProperties,
    @Nullable String deadLetterTopic,
    @DefaultValue("true") boolean fallback,
    @DefaultValue("true") boolean clientIdSuffix
) {

    public KafkaOutboxProperties {
        driverProperties = driverProperties != null ? driverProperties : Map.of();
    }

    /** The driver properties' keys only: their values can hold secrets, such as {@code sasl.jaas.config}. */
    @Override
    public String toString() {
        return "KafkaOutboxProperties[driverProperties=" + driverProperties.keySet() + ", deadLetterTopic="
            + deadLetterTopic + ", fallback=" + fallback + ", clientIdSuffix=" + clientIdSuffix + "]";
    }
}
