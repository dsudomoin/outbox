package io.github.dsudomoin.outbox.kafka.spring;

import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.kafka.KafkaDeadLetterHandler;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxConfig;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxHandler;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxSender;
import java.util.Properties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Relays the outbox to Kafka, settings under {@code outbox.kafka.*}: a {@link KafkaOutboxSender}, a
 * {@link KafkaOutboxHandler} for every topic no {@link OutboxTopicHandler} bean claims, and, with
 * {@code outbox.kafka.dead-letter-topic} set, a {@link KafkaDeadLetterHandler} that publishes dead letters; without it
 * they stay in the dead table. An app's own {@link KafkaOutboxSender} or {@link OutboxDeadLetterHandler} bean replaces
 * that one. An app's own {@link OutboxHandler} bean fails the start, so that it cannot take Kafka's topics by
 * accident, unless {@code outbox.kafka.fallback=false} lets it.
 * <p>
 * The beans are lazy, as Kora builds only what the graph's roots reach: {@code bootstrap.servers} is required once
 * something uses the sender (the outbox's fallback or dead-letter hook, or an app's bean), and fails the start then,
 * and not with {@code outbox.enabled=false} or a routes-only outbox.
 */
@AutoConfiguration
@EnableConfigurationProperties(KafkaOutboxProperties.class)
public final class KafkaOutboxAutoConfiguration {

    /** {@code outbox} is the relay's config, optional as in Kora's module: a Kafka-only app has none. */
    @Bean(destroyMethod = "close")
    @Lazy
    @ConditionalOnMissingBean(KafkaOutboxSender.class)
    KafkaOutboxSender kafkaOutboxSender(KafkaOutboxProperties properties, ObjectProvider<OutboxConfig> outbox) {
        var driver = new Properties();
        driver.putAll(properties.driverProperties());
        KafkaOutboxConfig config = () -> driver;
        var sender = new KafkaOutboxSender(config);
        outbox.ifAvailable(o -> sender.warnIfLongerThan("outbox.relay.batchTimeout", o.relay().batchTimeout(),
            "The relay interrupts a call at 80% of batchTimeout; the sender then stops at once, keeps acked records as"
                + " delivered and retries the rest, so slow sends end as retries"));
        return sender;
    }

    /**
     * {@code outbox.kafka.fallback} read as the binder reads it: {@code @ConditionalOnProperty} matches anything but
     * {@code false}, so {@code 0}, {@code off} or {@code no} would keep Kafka's handler while the property says it is
     * off. A value that is no boolean fails the start.
     */
    static final class FallbackCondition extends SpringBootCondition {

        @Override
        public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
            boolean fallback = Binder.get(context.getEnvironment()).bind("outbox.kafka.fallback", Boolean.class)
                .orElse(true);
            return new ConditionOutcome(fallback, "outbox.kafka.fallback=" + fallback);
        }
    }

    @Bean
    @Lazy
    @Conditional(FallbackCondition.class)
    KafkaOutboxHandler outboxKafkaFallbackHandler(KafkaOutboxSender sender) {
        return sender.handler();
    }

    /** The condition's kebab-case name also matches camelCase and environment variables, as the binder does. */
    @Bean
    @Lazy
    @ConditionalOnProperty(prefix = "outbox.kafka", name = "dead-letter-topic")
    @ConditionalOnMissingBean(OutboxDeadLetterHandler.class)
    KafkaDeadLetterHandler kafkaDeadLetterHandler(KafkaOutboxProperties properties, KafkaOutboxSender sender,
            ObjectProvider<OutboxConfig> outbox) {
        var hook = new KafkaDeadLetterHandler(sender, properties.deadLetterTopic());
        outbox.ifAvailable(o -> sender.warnIfLongerThan("outbox.deadLetters.timeout", o.deadLetters().timeout(),
            "The dead-letter pass abandons a hook call at its timeout and hands its letters again after a backoff, so"
                + " slow dead-letter sends end as retries and letters acked meanwhile repeat on their dead-letter"
                + " topic"));
        return hook;
    }
}
