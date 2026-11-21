package io.github.dsudomoin.outbox.kafka;

import static io.github.dsudomoin.outbox.testing.Utf8.bytes;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.Hashing;
import io.github.dsudomoin.outbox.micrometer.ObservationOutboxTelemetry;
import io.github.dsudomoin.outbox.micrometer.OutboxDeliveryTracingObservationHandler;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.Await;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelBaggageManager;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The telemetry stage by stage on the framework-free path: {@link OutboxRuntime} over {@link JdbcTransactions}, with
 * {@link KafkaOutboxSender}'s handler and {@link ObservationOutboxTelemetry} over {@link OutboxMetrics}, traced through
 * the OpenTelemetry bridge of micrometer-tracing, against a real Postgres and a real broker.
 */
class KafkaTelemetryIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final String STEP = "outbox relay step";
    private static final String SETTLE = "outbox relay settle";
    private static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("outbox.message_id");
    private static final AttributeKey<String> SHARD = AttributeKey.stringKey("outbox.shard");
    private static final AttributeKey<String> SHARDS = AttributeKey.stringKey("outbox.shards");
    private static final AttributeKey<String> RECORDS = AttributeKey.stringKey("outbox.records");
    private static final AttributeKey<String> SYSTEM = AttributeKey.stringKey("messaging.system");

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final SdkTracerProvider provider = SdkTracerProvider.builder().setSampler(Sampler.alwaysOn())
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build();
    private final io.opentelemetry.api.trace.Tracer otel = provider.get("test");
    private final OtelCurrentTraceContext current = new OtelCurrentTraceContext();
    private final Tracer tracer = new OtelTracer(otel, current, event -> {},
            new OtelBaggageManager(current, List.of(), List.of()));
    private final Propagator propagator =
            new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()), otel);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final ObservationOutboxTelemetry telemetry = observed();
    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final List<AutoCloseable> closing = new ArrayList<>(); // senders and runtimes, closed last first

    @AfterEach
    void close() throws Exception {
        try {
            for (int i = closing.size() - 1; i >= 0; i--) {
                closing.get(i).close();
            }
        } finally {
            provider.close();
        }
    }

    /**
     * Three writers, each in a span of its own, send one message each; the relay hands the three records to the
     * handler in one call, and each keeps its own writer's trace through enqueue, delivery and the Kafka record.
     */
    @Test
    void oneCallWithRecordsFromThreeTracesKeepsEachTrace() throws Exception {
        var topic = TestKafka.createTopic(3);
        var handler = new Handler();
        var runtime = start(KafkaGuaranteesIT.config().relayEnabled(false), handler);
        var writers = new ArrayList<Span>();
        var ids = new ArrayList<UUID>();
        for (int i = 0; i < 3; i++) {
            var writer = tracer.nextSpan().name("writer " + i).start();
            var message = OutboxMessage.of(topic, "k" + i, bytes("m" + i));
            try (var scope = tracer.withSpan(writer)) {
                ids.add(transactions.inTx(c -> runtime.outbox().send(message)));
            } finally {
                writer.end();
            }
            writers.add(writer);
        }

        runtime.relay().drain();

        assertThat(handler.calls).as("handler calls").singleElement()
                .satisfies(call -> assertThat(call).containsExactlyInAnyOrderElementsOf(ids));
        var received = TestKafka.byMessageId(
                TestKafka.consumeUntil(topic, TIMEOUT, r -> TestKafka.byMessageId(r).keySet().containsAll(ids)));
        var step = span(s -> s.getName().equals(STEP));
        for (int i = 0; i < ids.size(); i++) {
            var writer = writers.get(i).context();
            var enqueue = span(s -> s.getName().equals("outbox enqueue " + topic)
                    && s.getParentSpanId().equals(writer.spanId()));
            var delivery = delivery(ids.get(i));
            assertThat(delivery.getParentSpanId()).as("delivery %d", i).isEqualTo(enqueue.getSpanId());
            assertThat(delivery.getTraceId()).as("delivery %d", i).isEqualTo(writer.traceId());
            assertThat(delivery.getLinks()).as("delivery %d", i).singleElement()
                    .satisfies(l -> assertThat(l.getSpanContext().getSpanId()).isEqualTo(step.getSpanId()));
            assertThat(TestKafka.traceparent(received.get(ids.get(i)))).as("Kafka record %d", i)
                    .isEqualTo("00-" + writer.traceId() + "-" + delivery.getSpanId() + "-"
                            + delivery.getSpanContext().getTraceFlags().asHex());
        }
    }

    /**
     * The meters of real sends as README «Метрики» defines them: three messages delivered at once, one retried once
     * then delivered, one permanent failure, one requeued once then delivered. {@code failed{outcome=retry}} counts
     * rows diverted to the retry table; with {@code headFailuresBeforeDivert} 1 the first failed attempt diverts.
     */
    @Test
    void countersThroughRealSends() throws Exception {
        var topic = TestKafka.createTopic(3);
        var handler = new Handler();
        handler.fail = r -> switch (r.keyAsString()) {
            case "retried" -> r.attempts() == 0 ? Outcome.retry(new IOException("down once")) : null;
            case "dead" -> Outcome.permanent(new IllegalArgumentException("refused for good"));
            case "requeued" -> r.requeues() == 0 ? Outcome.requeue() : null;
            default -> null;
        };
        var runtime = start(KafkaGuaranteesIT.config().headFailuresBeforeDivert(1), handler);
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(Stream.of("a", "b", "c", "retried", "dead",
                "requeued").map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));
        var dead = ids.get(4);

        Await.until(TIMEOUT, () -> counter("outbox.messages.delivered", "topic", topic) == 5
                && counter("outbox.messages.failed", "topic", topic, "outcome", "dead") == 1);
        assertThat(counter("outbox.messages.enqueued", "topic", topic)).isEqualTo(6);
        assertThat(counter("outbox.messages.failed", "topic", topic, "outcome", "retry")).isEqualTo(1);
        assertThat(counter("outbox.messages.requeued", "topic", topic)).isEqualTo(1);
        var sent = ids.stream().filter(id -> !id.equals(dead)).toList();
        var received = TestKafka.consumeUntil(topic, TIMEOUT, r -> TestKafka.byMessageId(r).keySet().containsAll(sent));
        assertThat(TestKafka.byMessageId(received)).doesNotContainKey(dead);
    }

    /**
     * {@code outbox.shard.lag} as maintenance publishes it: above 0 while a shard holds a record it has not delivered,
     * back to 0 once it delivered it.
     */
    @Test
    void lagGaugeMovesWithTheBacklog() throws Exception {
        var topic = TestKafka.createTopic(1);
        var handler = new Handler();
        var down = new AtomicBoolean(true);
        handler.fail = r -> down.get() ? Outcome.retry(new IOException("down")) : null;
        // never diverted to the retry table: the record holds its shard back until it is delivered
        var runtime = start(KafkaGuaranteesIT.config().headFailuresBeforeDivert(1_000), handler);
        transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(topic, "k", bytes("late"))));

        Await.until(TIMEOUT, () -> runtime.maintenance().runOnce() && lag() > 0);
        down.set(false);
        Await.until(TIMEOUT, () -> counter("outbox.messages.delivered", "topic", topic) == 1);
        Await.until(TIMEOUT, () -> runtime.maintenance().runOnce() && lag() == 0);
    }

    /**
     * A drained step is observed without error. A step whose backend dies after the sender returned fails (a handler
     * failure would not: it is an outcome): its observation records the error, and the worker counts the failed pass.
     */
    @Test
    void relayStepsAreObservedAndFailuresCounted() throws Exception {
        var topic = TestKafka.createTopic(3);
        var config = KafkaGuaranteesIT.config().relayEnabled(false);
        var drained = start(config, new Handler());
        transactions.inTx(c -> drained.outbox().send(OutboxMessage.of(topic, "k", bytes("drained"))));

        drained.relay().drain();

        var shard = Integer.toString(Hashing.shard(Hashing.keyHash(topic, bytes("k")), config.shards()));
        assertThat(spans(STEP)).singleElement().satisfies(step -> {
            assertThat(step.getAttributes().get(SHARDS).split(",")).contains(shard);
            // micrometer-tracing 1.7 sets OK on a span that ends without error, earlier versions leave it UNSET
            assertThat(step.getStatus().getStatusCode()).isIn(StatusCode.UNSET, StatusCode.OK);
        });
        assertThat(meters.get("outbox.relay.step").tag("error", "none").timer().count()).isEqualTo(1);
        assertThat(counter("outbox.relay.failures")).isZero();

        var handler = new Handler();
        var running = start(KafkaGuaranteesIT.config(), handler);
        var crashed = new AtomicBoolean();
        var terminated = new AtomicInteger();
        handler.afterSend = records -> {
            if (crashed.compareAndSet(false, true)) {
                terminated.set(KafkaGuaranteesIT.terminateStep(transactions, running, records.get(0).shard()));
            }
        };
        transactions.inTx(c -> running.outbox().send(OutboxMessage.of(topic, "k", bytes("crashed"))));

        Await.until(TIMEOUT, () -> counter("outbox.relay.failures") >= 1
                && spans(STEP).stream().anyMatch(s -> s.getStatus().getStatusCode() == StatusCode.ERROR));
        assertThat(terminated).as("the step's backend").hasValue(1);
    }

    /**
     * A message sent outside any span still stores trace context: the root enqueue span its send started, which
     * parents its delivery span, linked to the step. A row written without telemetry stores none: its delivery span is
     * a child of the relay step that delivers it, the current span on the stepping thread, and has no link.
     */
    @Test
    void aRecordWithoutTraceContextHasTheStepAsParentAndNoLink() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().relayEnabled(false);
        var runtime = start(config, new Handler());
        var untraced = start(OutboxRuntime.builder(KafkaGuaranteesIT.config().sameTablesAs(config).relayEnabled(false),
                transactions, r -> {}));
        var outsideAnySpan = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(topic, "a", bytes("a"))));
        var withoutContext = transactions.inTx(c -> untraced.outbox().send(OutboxMessage.of(topic, "b", bytes("b"))));

        runtime.relay().drain();

        var enqueue = span(s -> s.getName().equals("outbox enqueue " + topic));
        assertThat(enqueue.getParentSpanContext().isValid()).as("the enqueue span has a parent").isFalse();
        var stored = delivery(outsideAnySpan);
        assertThat(stored.getParentSpanId()).isEqualTo(enqueue.getSpanId());
        assertThat(stored.getLinks()).singleElement().satisfies(l -> assertThat(
                span(s -> s.getSpanId().equals(l.getSpanContext().getSpanId())).getName()).isEqualTo(STEP));
        var delivery = delivery(withoutContext);
        var parent = span(s -> s.getSpanId().equals(delivery.getParentSpanId()));
        assertThat(parent.getName()).isEqualTo(STEP);
        assertThat(delivery.getTraceId()).isEqualTo(parent.getTraceId());
        assertThat(delivery.getLinks()).as("no link to its own parent").isEmpty();
    }

    /** One {@code sendAll} of three: a producer span per message, each the parent of its own delivery span. */
    @Test
    void enqueueSpanPerMessage() throws Exception {
        var topic = TestKafka.createTopic(3);
        var config = KafkaGuaranteesIT.config().relayEnabled(false);
        var runtime = start(config, new Handler());
        var keys = List.of("a", "b", "c");
        var writer = tracer.nextSpan().name("writer").start();
        List<UUID> ids;
        try (var scope = tracer.withSpan(writer)) {
            ids = transactions.inTx(c -> runtime.outbox().sendAll(
                    keys.stream().map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));
        } finally {
            writer.end();
        }

        runtime.relay().drain();

        assertThat(spans("outbox enqueue " + topic)).hasSize(3).allSatisfy(s -> {
            assertThat(s.getKind()).isEqualTo(SpanKind.PRODUCER);
            assertThat(s.getParentSpanId()).isEqualTo(writer.context().spanId());
        });
        for (int i = 0; i < ids.size(); i++) {
            var id = ids.get(i).toString();
            var enqueue = span(s -> s.getName().equals("outbox enqueue " + topic)
                    && id.equals(s.getAttributes().get(MESSAGE_ID)));
            int shard = Hashing.shard(Hashing.keyHash(topic, bytes(keys.get(i))), config.shards());
            assertThat(enqueue.getAttributes().get(SHARD)).as(keys.get(i)).isEqualTo(Integer.toString(shard));
            assertThat(delivery(ids.get(i)).getParentSpanId()).as(keys.get(i)).isEqualTo(enqueue.getSpanId());
        }
    }

    /** The step is a client span with the records it fetched, and it ends after its settle, which ends at the commit. */
    @Test
    void stepSpanIsClientEndsAfterCommitAndParentsTheSettle() throws Exception {
        var topic = TestKafka.createTopic(1);
        var runtime = start(KafkaGuaranteesIT.config().relayEnabled(false), new Handler());
        transactions.inTx(c -> runtime.outbox().sendAll(List.of(
                OutboxMessage.of(topic, "k", bytes("1")), OutboxMessage.of(topic, "k", bytes("2")))));

        runtime.relay().drain();

        var step = span(s -> s.getName().equals(STEP));
        var settle = span(s -> s.getName().equals(SETTLE));
        assertThat(step.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(step.getAttributes().get(RECORDS)).isEqualTo("2");
        assertThat(step.getEndEpochNanos()).isGreaterThanOrEqualTo(settle.getEndEpochNanos());
        assertThat(settle.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(settle.getParentSpanId()).isEqualTo(step.getSpanId());
        assertThat(settle.getAttributes().get(RECORDS)).isEqualTo("2");
    }

    /**
     * The sender reports each broker ack as it arrives, so each record's delivery span ends there, not at the end of the
     * call: the call then waits {@code tail} before it returns, and every delivery span ends well before the settle
     * starts.
     */
    @Test
    void eachRecordEndsAtItsAck() throws Exception {
        var topic = TestKafka.createTopic(3);
        var handler = new Handler();
        var tail = Duration.ofMillis(400);
        handler.afterSend = records -> {
            try {
                Thread.sleep(tail.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
        var runtime = start(KafkaGuaranteesIT.config().relayEnabled(false), handler);
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(Stream.of("a", "b", "c")
                .map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));

        runtime.relay().drain();

        assertThat(handler.calls).as("handler calls").singleElement()
                .satisfies(call -> assertThat(call).containsExactlyInAnyOrderElementsOf(ids));
        var settle = span(s -> s.getName().equals(SETTLE));
        for (var id : ids) {
            assertThat(delivery(id).getEndEpochNanos()).as("delivery of %s", id)
                    .isLessThanOrEqualTo(settle.getStartEpochNanos() - tail.toNanos() / 2);
        }
    }

    @Test
    void messagingSystemIsOutbox() throws Exception {
        var topic = TestKafka.createTopic(1);
        var runtime = start(KafkaGuaranteesIT.config().relayEnabled(false), new Handler());
        var id = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(topic, "k", bytes("v"))));

        runtime.relay().drain();

        assertThat(List.of(span(s -> s.getName().equals("outbox enqueue " + topic)), span(s -> s.getName().equals(STEP)),
                span(s -> s.getName().equals(SETTLE)), delivery(id)))
                .allSatisfy(s -> assertThat(s.getAttributes().get(SYSTEM)).as(s.getName()).isEqualTo("outbox"));
        assertThat(meters.get("outbox.deliver").tag("messaging.system", "outbox").timer().count()).isEqualTo(1);
    }

    @Test
    void shardOnDeliverySpans() throws Exception {
        var topic = TestKafka.createTopic(3);
        var config = KafkaGuaranteesIT.config().relayEnabled(false);
        var runtime = start(config, new Handler());
        var keys = IntStream.range(0, 8).mapToObj(i -> "k" + i).toList();
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(
                keys.stream().map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));

        runtime.relay().drain();

        for (int i = 0; i < keys.size(); i++) {
            int shard = Hashing.shard(Hashing.keyHash(topic, bytes(keys.get(i))), config.shards());
            assertThat(delivery(ids.get(i)).getAttributes().get(SHARD)).as(keys.get(i))
                    .isEqualTo(Integer.toString(shard));
        }
    }

    /**
     * {@link KafkaOutboxSender#handler()} that keeps the message ids of each call: a record {@link #fail} answers for
     * gets that outcome instead of being sent (every test has one message per key, so no later record of its key is
     * in the call); {@link #afterSend} runs once the sender returned, before the relay sees the outcomes.
     */
    private final class Handler implements OutboxHandler {

        private final KafkaOutboxHandler kafka = sender().handler();
        final List<List<UUID>> calls = new CopyOnWriteArrayList<>();
        volatile Function<OutboxRecord, Outcome> fail = r -> null; // null: send it
        volatile Consumer<List<OutboxRecord>> afterSend = records -> {};

        @Override
        public void handle(OutboxRecord record) {
            kafka.handle(record);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            calls.add(records.stream().map(OutboxRecord::messageId).toList());
            var outcomes = new Outcome[records.size()];
            var sending = new ArrayList<OutboxRecord>();
            for (int i = 0; i < outcomes.length; i++) {
                outcomes[i] = fail.apply(records.get(i));
                if (outcomes[i] == null) {
                    sending.add(records.get(i));
                }
            }
            var sent = (sending.isEmpty() ? List.<Outcome>of() : kafka.handleBatch(sending)).iterator();
            for (int i = 0; i < outcomes.length; i++) {
                if (outcomes[i] == null) {
                    outcomes[i] = sent.next();
                }
            }
            afterSend.accept(records);
            return Arrays.asList(outcomes);
        }
    }

    /**
     * As an application wires it: propagating sender handler, the outbox's delivery handler before the plain receiver
     * one, default tracing, meters.
     */
    private ObservationOutboxTelemetry observed() {
        var observations = ObservationRegistry.create();
        observations.observationConfig()
                .observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                        new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                        new OutboxDeliveryTracingObservationHandler(tracer, propagator),
                        new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
                        new DefaultTracingObservationHandler(tracer)))
                .observationHandler(new DefaultMeterObservationHandler(meters));
        return ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator).build();
    }

    private OutboxRuntime start(TestOutboxConfig config, Handler handler) {
        return start(OutboxRuntime.builder(config, transactions, handler).telemetry(telemetry));
    }

    private OutboxRuntime start(OutboxRuntime.Builder builder) {
        var runtime = builder.build();
        closing.add(runtime);
        runtime.start();
        return runtime;
    }

    private KafkaOutboxSender sender() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        TestKafka.shortTimeouts(properties);
        var sender = new KafkaOutboxSender(() -> properties);
        closing.add(sender);
        return sender;
    }

    private List<SpanData> spans(String name) {
        return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).toList();
    }

    /** The one finished span matching {@code filter}. */
    private SpanData span(Predicate<SpanData> filter) {
        var found = spans.getFinishedSpanItems().stream().filter(filter).toList();
        assertThat(found).hasSize(1);
        return found.get(0);
    }

    private SpanData delivery(UUID messageId) {
        return span(s -> s.getName().startsWith("outbox deliver ")
                && messageId.toString().equals(s.getAttributes().get(MESSAGE_ID)));
    }

    private double lag() {
        return meters.get("outbox.shard.lag").gauge().value();
    }

    /** The count of the counter, 0 while it is not registered. */
    private double counter(String name, String... tags) {
        var counter = meters.find(name).tags(tags).counter();
        return counter == null ? 0 : counter.count();
    }
}
