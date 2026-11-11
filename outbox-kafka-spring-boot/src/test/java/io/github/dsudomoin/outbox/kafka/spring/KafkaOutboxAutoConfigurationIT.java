package io.github.dsudomoin.outbox.kafka.spring;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.dsudomoin.outbox.DeadLetter;
import io.github.dsudomoin.outbox.DeadLetterDecision;
import io.github.dsudomoin.outbox.DeadLetterFilter;
import io.github.dsudomoin.outbox.Outbox;
import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.OutboxConfig;
import io.github.dsudomoin.outbox.OutboxDeadLetterHandler;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.kafka.KafkaDeadLetterHandler;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxConfig;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxHeaders;
import io.github.dsudomoin.outbox.kafka.KafkaOutboxSender;
import io.github.dsudomoin.outbox.kafka.TestKafka;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestSql;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/** The auto-configuration against real Kafka and Postgres, an app of Boot's auto-configurations around it. */
class KafkaOutboxAutoConfigurationIT {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final String BATCH_TIMEOUT_EFFECT = "The relay interrupts a call at 80% of batchTimeout; the sender"
        + " then stops at once, keeps acked records as delivered and retries the rest, so slow sends end as retries";
    private static final String DEAD_LETTERS_EFFECT = "The dead-letter pass abandons a hook call at its timeout and"
        + " hands its letters again after a backoff, so slow dead-letter sends end as retries and letters acked"
        + " meanwhile repeat on their dead-letter topic";

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration
    static class Boot {}

    /** Boot's datasource keys, no migrations, a fresh outbox table, the tests' fast relay settings; no Kafka. */
    private static ApplicationContextRunner withoutKafka(String... more) {
        var properties = Stream.concat(Stream.of("spring.datasource.url=" + TestDataSources.jdbcUrl(),
            "spring.datasource.username=" + TestDataSources.user(),
            "spring.datasource.password=" + TestDataSources.password(),
            "spring.flyway.enabled=false", "spring.liquibase.enabled=false",
            "outbox.table=" + TestSql.uniqueTable(), "outbox.inbox.table=" + TestSql.uniqueTable(),
            "outbox.shards=4", "outbox.relay.workers=2", "outbox.relay.idle-poll-interval=20ms",
            "outbox.relay.min-tick-interval=5ms", "outbox.relay.batch-timeout=5s",
            "outbox.relay.shutdown-timeout=5s", "outbox.relay.retry.initial-delay=50ms",
            "outbox.relay.retry.max-delay=500ms"), Stream.of(more)).toArray(String[]::new);
        return new ApplicationContextRunner().withUserConfiguration(Boot.class).withPropertyValues(properties);
    }

    private static ApplicationContextRunner runner(String... more) {
        return withoutKafka(more)
            .withPropertyValues("outbox.kafka.driver-properties.bootstrap.servers=" + TestKafka.bootstrapServers());
    }

    private static UUID send(ApplicationContext context, OutboxMessage message) {
        var outbox = context.getBean(Outbox.class);
        return new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
            .execute(status -> outbox.send(message));
    }

    private static UUID send(ApplicationContext context, String topic) {
        return send(context, OutboxMessage.of(topic, "k", "v".getBytes(UTF_8)));
    }

    private static String failure(AssertableApplicationContext context) {
        assertThat(context).hasFailed();
        var cause = context.getStartupFailure();
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage();
    }

    /** A topic whose broker limit refuses {@link #tooLarge} for good, and its dead-letter topic with Kafka's limit. */
    private static String refusingTopic() {
        var topic = TestKafka.createTopic("t-" + UUID.randomUUID(), 1, Map.of("max.message.bytes", "1024"));
        TestKafka.createTopic(topic + ".dlq", 1, Map.of());
        return topic;
    }

    private static OutboxMessage tooLarge(String topic) {
        return OutboxMessage.of(topic, "k", new byte[10 * 1024]);
    }

    static class Events {

        private final Outbox outbox;

        Events(Outbox outbox) {
            this.outbox = outbox;
        }

        @Transactional
        public UUID publish(String topic) {
            return outbox.send(OutboxMessage.of(topic, "k", "v".getBytes(UTF_8)).withHeader("h", "a".getBytes(UTF_8)));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithEvents {

        @Bean
        Events events(Outbox outbox) {
            return new Events(outbox);
        }
    }

    @Test
    void endToEnd() {
        var topic = TestKafka.createTopic(1);
        runner().withUserConfiguration(WithEvents.class).run(context -> {
            var id = context.getBean(Events.class).publish(topic);

            var record = TestKafka.consumeUntil(topic, WAIT, r -> !r.isEmpty()).get(0);
            assertThat(record.key()).isEqualTo("k".getBytes(UTF_8));
            assertThat(record.value()).isEqualTo("v".getBytes(UTF_8));
            assertThat(record.headers().lastHeader("h").value()).isEqualTo("a".getBytes(UTF_8));
            assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(id);
        });
    }

    /** The route of topic {@code audit}, recording what it is handed. */
    static final class AuditRoute implements OutboxTopicHandler {

        final RecordingHandler records = new RecordingHandler();

        @Override
        public Set<String> topics() {
            return Set.of("audit");
        }

        @Override
        public void handle(OutboxRecord record) throws Exception {
            records.handle(record);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithRoute {

        @Bean
        AuditRoute auditRoute() {
            return new AuditRoute();
        }
    }

    @Test
    void routesAndFallbackTogether() {
        var topic = TestKafka.createTopic(1);
        try (var err = new CapturedStderr()) {
            runner().withUserConfiguration(WithRoute.class).run(context -> {
                var audit = context.getBean(AuditRoute.class).records;
                var routed = send(context, "audit");
                var unrouted = send(context, topic);

                var record = TestKafka.consumeUntil(topic, WAIT, r -> !r.isEmpty()).get(0);
                assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(unrouted);
                await().atMost(WAIT).until(() -> !audit.delivered().isEmpty());
                assertThat(audit.delivered()).extracting(OutboxRecord::messageId).containsExactly(routed);
                assertThat(err.text())
                    .contains("outbox routes: audit -> auditRoute; other topics -> outboxKafkaFallbackHandler");
            });
        }
    }

    static final class AppHandler implements OutboxHandler {

        final RecordingHandler records = new RecordingHandler();

        @Override
        public void handle(OutboxRecord record) throws Exception {
            records.handle(record);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithAppHandler {

        @Bean
        AppHandler appHandler() {
            return new AppHandler();
        }
    }

    private static void assertNamesTheConflict(String failure) {
        assertThat(failure)
            .contains("several OutboxHandler beans: ").contains("outboxKafkaFallbackHandler")
            .contains("appHandler (" + AppHandler.class.getName() + ")")
            .endsWith("; outbox-kafka-spring-boot provides outboxKafkaFallbackHandler while"
                + " outbox.kafka.fallback=true: set outbox.kafka.fallback=false to let your own handler take them");
    }

    @Test
    void appOutboxHandlerWithFallbackFails() {
        runner().withUserConfiguration(WithAppHandler.class)
            .run(context -> assertNamesTheConflict(failure(context)));
    }

    /** The conflict comes first: a sender that cannot be built must not hide it behind advice to configure Kafka. */
    @Test
    void appOutboxHandlerWithFallbackFailsWhateverTheSender() {
        withoutKafka().withUserConfiguration(WithAppHandler.class)
            .run(context -> assertNamesTheConflict(failure(context)));
        runner("outbox.kafka.driver-properties.acks=1").withUserConfiguration(WithAppHandler.class)
            .run(context -> assertNamesTheConflict(failure(context)));
    }

    @Configuration(proxyBeanMethods = false)
    static class WithSubHandlers {

        @Bean(defaultCandidate = false)
        OutboxHandler notByDefault() {
            return new AppHandler();
        }

        @Bean(autowireCandidate = false)
        OutboxHandler neverInjected() {
            return new AppHandler();
        }
    }

    /** Sub-handlers the outbox's lookup does not see are no second OutboxHandler. */
    @Test
    void nonCandidateHandlersBesideFallbackStart() {
        var topic = TestKafka.createTopic(1);
        runner().withUserConfiguration(WithSubHandlers.class).run(context -> {
            assertThat(context).hasNotFailed();
            var id = send(context, topic);

            var record = TestKafka.consumeUntil(topic, WAIT, r -> !r.isEmpty()).get(0);
            assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(id);
        });
    }

    @Test
    void fallbackFalseUsesAppHandler() {
        runner("outbox.kafka.fallback=false").withUserConfiguration(WithAppHandler.class).run(context -> {
            assertThat(context).doesNotHaveBean("outboxKafkaFallbackHandler");
            assertThat(context).hasSingleBean(KafkaOutboxSender.class);

            var id = send(context, "anything");

            var records = context.getBean(AppHandler.class).records;
            await().atMost(WAIT).until(() -> !records.delivered().isEmpty());
            assertThat(records.delivered()).extracting(OutboxRecord::messageId).containsExactly(id);
        });
    }

    /** The switch decides as the binder binds it, whatever the spelling. */
    @ParameterizedTest
    @ValueSource(strings = {"0", "off", "no", "FALSE"})
    void fallbackOffInAnySpellingUsesAppHandler(String off) {
        withoutKafka("outbox.kafka.fallback=" + off).withUserConfiguration(WithAppHandler.class).run(context -> {
            assertThat(context).hasNotFailed().doesNotHaveBean("outboxKafkaFallbackHandler");
            assertThat(context.getBean(KafkaOutboxProperties.class).fallback()).isFalse();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "on", "yes", "TRUE"})
    void fallbackOnInAnySpellingKeepsKafka(String on) {
        withoutKafka("outbox.kafka.fallback=" + on).withUserConfiguration(WithAppHandler.class)
            .run(context -> assertNamesTheConflict(failure(context)));
    }

    @Test
    void fallbackUnparseableFailsTheStart() {
        withoutKafka("outbox.kafka.fallback=flase").withUserConfiguration(WithAppHandler.class).run(context ->
            assertThat(context).hasFailed().getFailure().rootCause().hasMessageContaining("flase"));
    }

    /** Nothing uses Kafka here: no Kafka settings are needed. */
    @Test
    void fallbackFalseRoutesOnly() {
        withoutKafka("outbox.kafka.fallback=false").withUserConfiguration(WithRoute.class).run(context -> {
            assertThat(context).doesNotHaveBean(OutboxHandler.class);

            // caught inside, so that the transaction commits: a row written before the refusal would stay
            var outbox = context.getBean(Outbox.class);
            new TransactionTemplate(context.getBean(PlatformTransactionManager.class))
                .executeWithoutResult(status -> assertThatThrownBy(() ->
                    outbox.send(OutboxMessage.of("unrouted", "k", "v".getBytes(UTF_8))))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unrouted"));
            try (var c = TestDataSources.newDataSource().getConnection()) {
                assertThat(TestSql.queryLong(c, "select count(*) from " + context.getBean(OutboxConfig.class).table()))
                    .isZero();
            }

            var routed = send(context, "audit");
            var audit = context.getBean(AuditRoute.class).records;
            await().atMost(WAIT).until(() -> !audit.delivered().isEmpty());
            assertThat(audit.delivered()).extracting(OutboxRecord::messageId).containsExactly(routed);
        });
    }

    @Test
    void deadLetterTopicPublishes() {
        var topic = refusingTopic();
        runner("outbox.kafka.dead-letter-topic={topic}.dlq").run(context -> {
            var id = send(context, tooLarge(topic));

            var record = TestKafka.consumeUntil(topic + ".dlq", WAIT, r -> !r.isEmpty()).get(0);
            assertThat(KafkaOutboxHeaders.messageId(record)).isEqualTo(id);
            assertThat(record.headers().lastHeader(KafkaDeadLetterHandler.DEAD_TOPIC).value())
                .isEqualTo(topic.getBytes(UTF_8));
        });
    }

    @Test
    void camelCaseDeadLetterTopicActivatesTheHook() {
        runner("outbox.kafka.deadLetterTopic={topic}.dlq")
            .run(context -> assertThat(context.getBean(KafkaDeadLetterHandler.class).active()).isTrue());
    }

    /** No hook bean to leave in the slot of a later one: the core's default keeps the letter, with no call. */
    @Test
    void withoutDeadLetterTopicThereIsNoHookAndLettersAreKept() {
        var topic = refusingTopic();
        runner().run(context -> {
            assertThat(context).doesNotHaveBean(OutboxDeadLetterHandler.class);

            send(context, tooLarge(topic));

            var admin = context.getBean(OutboxAdmin.class);
            await().atMost(WAIT).until(() -> admin.countDeadLetters(DeadLetterFilter.any()) == 1);
        });
    }

    static final class AppHook implements OutboxDeadLetterHandler {

        final List<DeadLetter> letters = new CopyOnWriteArrayList<>();

        @Override
        public List<DeadLetterDecision> handle(List<DeadLetter> handed) {
            letters.addAll(handed);
            return handed.stream().map(letter -> (DeadLetterDecision) DeadLetterDecision.keep()).toList();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WithAppHook {

        @Bean
        AppHook appHook() {
            return new AppHook();
        }
    }

    @Test
    void appDeadLetterHookReplacesKafkas() {
        var topic = refusingTopic();
        runner("outbox.kafka.dead-letter-topic={topic}.dlq").withUserConfiguration(WithAppHook.class).run(context -> {
            assertThat(context).doesNotHaveBean(KafkaDeadLetterHandler.class);

            var id = send(context, tooLarge(topic));

            var letters = context.getBean(AppHook.class).letters;
            await().atMost(WAIT).until(() -> !letters.isEmpty());
            assertThat(letters).extracting(DeadLetter::messageId).containsExactly(id);
        });
    }

    @Test
    void dottedDriverPropertiesBind() {
        withoutKafka("outbox.enabled=false", "outbox.kafka.driver-properties.bootstrap.servers=a:1",
            "outbox.kafka.driver-properties[max.block.ms]=1000", "outbox.kafka.driverProperties.linger.ms=7")
            .run(context -> {
                var properties = context.getBean(KafkaOutboxProperties.class);
                assertThat(properties.driverProperties()).containsOnly(Map.entry("bootstrap.servers", "a:1"),
                    Map.entry("max.block.ms", "1000"), Map.entry("linger.ms", "7"));
                assertThat(properties.deadLetterTopic()).isNull();
                assertThat(properties.fallback()).isTrue();
                assertThat(properties.clientIdSuffix()).isTrue();
            });
    }

    @Test
    void toStringLeavesOutDriverValues() {
        var properties = new KafkaOutboxProperties(Map.of("sasl.jaas.config", "secret"), "{topic}.dlq", true, true);
        assertThat(properties.toString())
            .doesNotContain("secret").contains("sasl.jaas.config", "{topic}.dlq", "fallback=true");
    }

    @Test
    void shortBatchTimeoutWarns() {
        try (var err = new CapturedStderr()) {
            runner("outbox.relay.batch-timeout=5s").run(context -> assertThat(context).hasNotFailed());
            assertThat(err.text()).contains("WARN").contains("outbox.relay.batchTimeout (PT5S)")
                .contains(BATCH_TIMEOUT_EFFECT);
        }
        try (var err = new CapturedStderr()) {
            runner("outbox.relay.batch-timeout=30s").run(context -> assertThat(context).hasNotFailed());
            assertThat(err.text()).doesNotContain(BATCH_TIMEOUT_EFFECT);
        }
    }

    /** Only with the hook active: without a dead-letter topic no call is bounded by that timeout. */
    @Test
    void shortDeadLetterTimeoutWarns() {
        try (var err = new CapturedStderr()) {
            runner("outbox.dead-letters.timeout=5s", "outbox.kafka.dead-letter-topic={topic}.dlq")
                .run(context -> assertThat(context).hasNotFailed());
            assertThat(err.text()).contains("WARN").contains("outbox.deadLetters.timeout (PT5S)")
                .contains(DEAD_LETTERS_EFFECT);
        }
        try (var err = new CapturedStderr()) {
            runner("outbox.dead-letters.timeout=5s").run(context -> assertThat(context).hasNotFailed());
            assertThat(err.text()).doesNotContain(DEAD_LETTERS_EFFECT);
        }
    }

    /** As Kora builds only what the graph's roots reach: without an outbox nothing uses the sender. */
    @Test
    void outboxDisabledNeedsNoKafkaSettings() {
        withoutKafka("outbox.enabled=false").run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).doesNotHaveBean(Outbox.class);
        });
    }

    @Test
    void outboxDisabledLeavesTheSenderToTheApp() {
        var topic = TestKafka.createTopic(1);
        runner("outbox.enabled=false").run(context -> {
            var sender = context.getBean(KafkaOutboxSender.class);
            var record = new OutboxRecord(1, UUID.randomUUID(), topic, null, "v".getBytes(UTF_8), List.of(),
                Instant.now(), 0, 0);

            sender.send(List.of(record));

            TestKafka.consumeUntil(topic, WAIT, r -> !r.isEmpty());
        });
    }

    /**
     * The auto-configured sender builds its producers with what {@link KafkaOutboxSender} forces and defaults, as the
     * producer's own config log shows it: the adapter binds {@code driver-properties} and adds nothing.
     */
    @Test
    void theSendersProducersGetTheForcedSettings() {
        var topic = TestKafka.createTopic(1);
        try (var err = new CapturedStderr()) {
            runner("outbox.enabled=false",
                "outbox.kafka.driver-properties.key.serializer=org.apache.kafka.common.serialization.StringSerializer")
                .run(context -> {
                    var record = new OutboxRecord(1, UUID.randomUUID(), topic, null, "v".getBytes(UTF_8), List.of(),
                        Instant.now(), 0, 0);
                    context.getBean(KafkaOutboxSender.class).send(List.of(record));
                });
            assertThat(err.text()).contains("ProducerConfig values", "acks = -1", "enable.idempotence = true",
                "delivery.timeout.ms = 20000", "request.timeout.ms = 15000", "max.block.ms = 5000",
                "key.serializer = class org.apache.kafka.common.serialization.ByteArraySerializer");
        }
    }

    /** The producer's own log names it by the set client.id as it is with client-id-suffix=false, suffixed without. */
    @Test
    void clientIdSuffixOffKeepsTheClientId() {
        var topic = TestKafka.createTopic(1);
        for (var suffix : List.of(true, false)) {
            try (var err = new CapturedStderr()) {
                runner("outbox.enabled=false", "outbox.kafka.driver-properties.client.id=orders-svc",
                    "outbox.kafka.client-id-suffix=" + suffix)
                    .run(context -> {
                        var record = new OutboxRecord(1, UUID.randomUUID(), topic, null, "v".getBytes(UTF_8),
                            List.of(), Instant.now(), 0, 0);
                        context.getBean(KafkaOutboxSender.class).send(List.of(record));
                    });
                assertThat(err.text()).contains("[Producer clientId=orders-svc");
                assertThat(err.text().contains("[Producer clientId=orders-svc]")).as("client-id-suffix=" + suffix)
                    .isEqualTo(!suffix);
            }
        }
    }

    @Test
    void kafkaInUseWithoutServersFails() {
        withoutKafka().run(context -> assertThat(failure(context)).contains("bootstrap.servers is required"));
        withoutKafka("outbox.kafka.fallback=false", "outbox.kafka.dead-letter-topic={topic}.dlq")
            .withUserConfiguration(WithRoute.class)
            .run(context -> assertThat(failure(context)).contains("bootstrap.servers is required"));
    }

    @Test
    void invalidDeadLetterTopicFails() {
        runner("outbox.kafka.dead-letter-topic={topic}")
            .run(context -> assertThat(failure(context)).contains("outbox.kafka.deadLetterTopic"));
    }

    /** The stop of a restart leaves the sender open: the next runtime delivers through it. */
    @Test
    void deliversAfterRestart() {
        var topic = TestKafka.createTopic(1);
        runner().run(context -> {
            var a = send(context, topic);
            TestKafka.consumeUntil(topic, WAIT, r -> r.size() == 1);

            context.stop();
            context.start();

            var b = send(context, topic);
            var ids = TestKafka.consumeUntil(topic, WAIT,
                r -> r.stream().map(KafkaOutboxHeaders::messageId).toList().contains(b))
                .stream().map(KafkaOutboxHeaders::messageId).toList();
            assertThat(ids).as("A before the restart, B after it").contains(a);
            assertThat(ids.indexOf(a)).isLessThan(ids.indexOf(b));
        });
    }

    /** What the outbox's lifecycle was doing whenever a producer closed. */
    static final class ProducerCloses {

        volatile SmartLifecycle lifecycle;
        final List<Boolean> outboxRunning = new CopyOnWriteArrayList<>();
    }

    @Configuration(proxyBeanMethods = false)
    static class WithOwnSender {

        @Bean
        ProducerCloses producerCloses() {
            return new ProducerCloses();
        }

        @Bean
        KafkaOutboxSender ownSender(ProducerCloses closes) {
            var properties = new Properties();
            properties.put("bootstrap.servers", TestKafka.bootstrapServers());
            KafkaOutboxConfig config = () -> properties;
            return new KafkaOutboxSender(config, producerProperties -> observed(producerProperties, closes));
        }

        @SuppressWarnings("unchecked")
        private static Producer<byte[], byte[]> observed(Map<String, Object> properties, ProducerCloses closes) {
            var producer = new KafkaProducer<byte[], byte[]>(properties);
            return (Producer<byte[], byte[]>) Proxy.newProxyInstance(Producer.class.getClassLoader(),
                new Class<?>[] {Producer.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        closes.outboxRunning.add(closes.lifecycle.isRunning());
                    }
                    try {
                        return method.invoke(producer, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        }
    }

    /** An app's sender replaces the default and, being a bean, closes after the outbox has stopped. */
    @Test
    void appSenderIsUsedAndClosesAfterTheRuntime() {
        var topic = TestKafka.createTopic(1);
        runner().withUserConfiguration(WithOwnSender.class).run(context -> {
            assertThat(context.getBeansOfType(KafkaOutboxSender.class)).containsOnlyKeys("ownSender");
            var closes = context.getBean(ProducerCloses.class);
            closes.lifecycle = context.getBean("outboxLifecycle", SmartLifecycle.class);
            assertThat(closes.lifecycle.isRunning()).isTrue();
            send(context, topic);
            TestKafka.consumeUntil(topic, WAIT, r -> !r.isEmpty());

            context.close();

            assertThat(closes.outboxRunning).isNotEmpty().containsOnly(false);
        });
    }
}
