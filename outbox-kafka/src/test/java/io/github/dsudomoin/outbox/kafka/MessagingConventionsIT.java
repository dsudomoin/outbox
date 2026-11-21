package io.github.dsudomoin.outbox.kafka;

import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.BATCH_COUNT;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.COLLECTION;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.DEAD_LETTERED;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.DELIVERED;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.DESTINATION;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.ENQUEUED;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.ERROR_TYPE;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.FETCH_BATCH_SIZE;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.OPERATION_NAME;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.OPERATION_TYPE;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.PARTITION;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.RECORD_ID;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.RESEND_COUNT;
import static io.github.dsudomoin.outbox.kafka.MessagingConventionsTelemetry.SYSTEM;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.JdbcTransactions;
import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxRuntime;
import io.github.dsudomoin.outbox.OutboxSqlException;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Conformance of the public SPI: {@link MessagingConventionsTelemetry}, a messaging-semantic-conventions model built
 * on {@link io.github.dsudomoin.outbox.OutboxTelemetry} and the OpenTelemetry API alone, over {@link OutboxRuntime}
 * with {@link JdbcTransactions} and {@link KafkaOutboxSender}'s handler, against a real Postgres and a real broker.
 * The sender's producer is wrapped as a Kafka client instrumentation would wrap it: a {@code send <topic>} PRODUCER
 * span per record, child of the current context, injected into the record's headers. A failing check here is a gap in
 * the SPI, not in this model.
 */
class MessagingConventionsIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    /** One header whose value length (2) runs past the one byte stored: the relay's decode refuses it. */
    private static final byte[] CORRUPT_HEADERS = {0, 1, 0, 1, 'a', 0, 0, 0, 2, 0};

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final SdkTracerProvider provider = SdkTracerProvider.builder().setSampler(Sampler.alwaysOn())
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build();
    private final io.opentelemetry.api.trace.Tracer tracer = provider.get("test");
    private final TextMapPropagator propagator = W3CTraceContextPropagator.getInstance();
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
     * Two messages of one transaction, one delivered and one refused for good, on a single shard: every span with its
     * kind, name, parent and attributes, and every meter: enqueued, delivery attempts, the dead letter by
     * {@code error.type}, the fetch batch size, and the shard's pending and oldest-age gauges before and after.
     */
    @Test
    void expectedSpansAndMetrics() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().shards(1).relayEnabled(false);
        var telemetry = telemetry(config);
        var handler = new Handler();
        handler.fail = r -> r.keyAsString().equals("dead") ? Outcome.permanent(new IllegalArgumentException("refused"))
                : null;
        var runtime = start(config, telemetry, handler);
        var writer = tracer.spanBuilder("writer").startSpan();
        UUID ok;
        UUID dead;
        try (var scope = writer.makeCurrent()) {
            var ids = transactions.inTx(c -> List.of(
                    runtime.outbox().send(OutboxMessage.of(topic, "ok", bytes("ok"))),
                    runtime.outbox().send(OutboxMessage.of(topic, "dead", bytes("dead")))));
            ok = ids.get(0);
            dead = ids.get(1);
        } finally {
            writer.end();
        }
        var meters = telemetry.meters();
        await(() -> runtime.maintenance().runOnce() && meters.pending(0) == 2);
        assertThat(meters.oldestSeconds(0)).as("oldest age with two pending").isGreaterThanOrEqualTo(0);

        runtime.relay().drain();

        var table = config.table();
        var fetch = fetch(table);
        assertThat(fetch.getKind()).isEqualTo(SpanKind.CLIENT);
        assertCommon(fetch, table, "fetch", "receive");
        assertThat(fetch.getAttributes().get(BATCH_COUNT)).isEqualTo(2);
        assertThat(fetch.getAttributes().get(PARTITION)).as("the step's one shard").isEqualTo("0");
        var settle = span(s -> s.getName().equals("settle " + table) && s.getParentSpanId().equals(fetch.getSpanId()));
        assertThat(settle.getKind()).isEqualTo(SpanKind.CLIENT);
        assertCommon(settle, table, "settle", "settle");
        assertThat(settle.getAttributes().get(BATCH_COUNT)).isEqualTo(2);
        assertThat(fetch.getEndEpochNanos()).as("the step ends after its commit").isGreaterThanOrEqualTo(
                settle.getEndEpochNanos());
        for (var id : List.of(ok, dead)) {
            var enqueue = enqueue(id);
            assertThat(enqueue.getName()).isEqualTo("enqueue " + topic);
            assertThat(enqueue.getKind()).isEqualTo(SpanKind.PRODUCER);
            assertThat(enqueue.getParentSpanId()).isEqualTo(writer.getSpanContext().getSpanId());
            assertCommon(enqueue, table, "enqueue", "send");
            assertThat(enqueue.getAttributes().get(DESTINATION)).isEqualTo(topic);
            assertThat(enqueue.getAttributes().get(PARTITION)).isEqualTo("0");
            var deliver = deliver(id);
            assertThat(deliver.getName()).isEqualTo("deliver " + topic);
            assertThat(deliver.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertCommon(deliver, table, "deliver", "process");
            assertThat(deliver.getAttributes().get(DESTINATION)).isEqualTo(topic);
            assertThat(deliver.getAttributes().get(PARTITION)).isEqualTo("0");
            assertThat(deliver.getAttributes().get(RESEND_COUNT)).as("a first attempt").isNull();
            assertThat(deliver.getParentSpanId()).isEqualTo(enqueue.getSpanId());
        }
        assertThat(deliver(ok).getAttributes().get(ERROR_TYPE)).isNull();
        assertThat(deliver(ok).getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        assertThat(deliver(dead).getAttributes().get(ERROR_TYPE)).isEqualTo(IllegalArgumentException.class.getName());
        assertThat(deliver(dead).getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);

        var dest = DESTINATION.getKey();
        assertThat(meters.count(ENQUEUED, dest, topic)).isEqualTo(2);
        assertThat(meters.count(DELIVERED, dest, topic)).isEqualTo(1);
        assertThat(meters.count(DELIVERED, dest, topic, ERROR_TYPE.getKey(), IllegalArgumentException.class.getName()))
                .isEqualTo(1);
        assertThat(meters.count(DEAD_LETTERED, dest, topic, ERROR_TYPE.getKey(),
                IllegalArgumentException.class.getName())).isEqualTo(1);
        assertThat(meters.histogram(FETCH_BATCH_SIZE)).as("the steps with records").contains(2L)
                .allMatch(n -> n == 2 || n == 0);
        assertFetchSpansHaveRecords();
        await(() -> runtime.maintenance().runOnce() && meters.pending(0) == 0);
        assertThat(meters.oldestSeconds(0)).as("oldest age with none pending").isZero();
    }

    /**
     * The relay runs in another runtime with a telemetry of its own, so nothing but the row carries the writer's
     * trace: the record handed to the handler stores the enqueue span's context, and that span parents the delivery.
     */
    @Test
    void contextIsRestoredFromTheRow() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().relayEnabled(false);
        var writing = start(config, telemetry(config), new Handler());
        var handler = new Handler();
        var relaying = start(KafkaGuaranteesIT.config().sameTablesAs(config).relayEnabled(false), telemetry(config),
                handler);
        var writer = tracer.spanBuilder("writer").startSpan();
        UUID id;
        try (var scope = writer.makeCurrent()) {
            id = transactions.inTx(c -> writing.outbox().send(OutboxMessage.of(topic, "k", bytes("v"))));
        } finally {
            writer.end();
        }

        relaying.relay().drain();

        var enqueue = enqueue(id);
        var record = handler.records.stream().filter(r -> r.messageId().equals(id)).findFirst().orElseThrow();
        assertThat(traceparent(record)).as("the stored context")
                .isEqualTo("00-" + enqueue.getTraceId() + "-" + enqueue.getSpanId() + "-01");
        var deliver = deliver(id);
        assertThat(deliver.getParentSpanId()).isEqualTo(enqueue.getSpanId());
        assertThat(deliver.getTraceId()).isEqualTo(writer.getSpanContext().getTraceId());
    }

    /**
     * The Kafka producer span the sender's {@code send} starts is a child of the record's deliver span, current in the
     * record's scope, and it is the context the consumed Kafka record carries.
     */
    @Test
    void kafkaSpanIsAChildOfDeliver() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().relayEnabled(false);
        var runtime = start(config, telemetry(config), new Handler());
        var writer = tracer.spanBuilder("writer").startSpan();
        UUID id;
        try (var scope = writer.makeCurrent()) {
            id = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(topic, "k", bytes("v"))));
        } finally {
            writer.end();
        }

        runtime.relay().drain();

        var record = TestKafka.byMessageId(
                TestKafka.consumeUntil(topic, TIMEOUT, r -> TestKafka.byMessageId(r).containsKey(id))).get(id);
        var deliver = deliver(id);
        var send = span(s -> s.getName().equals("send " + topic));
        assertThat(send.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(send.getParentSpanId()).isEqualTo(deliver.getSpanId());
        assertThat(send.getTraceId()).isEqualTo(writer.getSpanContext().getTraceId());
        assertThat(TestKafka.traceparent(record)).as("the Kafka record's context")
                .isEqualTo("00-" + send.getTraceId() + "-" + send.getSpanId() + "-01");
        assertThat(deliver.getEndEpochNanos()).as("the delivery ends at the ack, after the send")
                .isGreaterThanOrEqualTo(send.getEndEpochNanos());
    }

    /**
     * A record with stored context: a child of its enqueue span, with one link, to its fetch span. A record written
     * without telemetry, in the same step: a child of the fetch span, with no link.
     */
    @Test
    void deliverLinksToFetch() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().shards(1).relayEnabled(false);
        var runtime = start(config, telemetry(config), new Handler());
        var untraced = start(OutboxRuntime.builder(
                KafkaGuaranteesIT.config().sameTablesAs(config).shards(1).relayEnabled(false), transactions, r -> {}));
        var writer = tracer.spanBuilder("writer").startSpan();
        UUID traced;
        try (var scope = writer.makeCurrent()) {
            traced = transactions.inTx(c -> runtime.outbox().send(OutboxMessage.of(topic, "a", bytes("a"))));
        } finally {
            writer.end();
        }
        var bare = transactions.inTx(c -> untraced.outbox().send(OutboxMessage.of(topic, "b", bytes("b"))));

        runtime.relay().drain();

        var fetch = fetch(config.table());
        var withContext = deliver(traced);
        assertThat(withContext.getParentSpanId()).isEqualTo(enqueue(traced).getSpanId());
        assertThat(withContext.getLinks()).singleElement().satisfies(l -> {
            assertThat(l.getSpanContext().getSpanId()).isEqualTo(fetch.getSpanId());
            assertThat(l.getSpanContext().getTraceId()).isEqualTo(fetch.getTraceId());
        });
        var withoutContext = deliver(bare);
        assertThat(withoutContext.getParentSpanId()).isEqualTo(fetch.getSpanId());
        assertThat(withoutContext.getTraceId()).isEqualTo(fetch.getTraceId());
        assertThat(withoutContext.getLinks()).as("no link to its own parent").isEmpty();
    }

    /**
     * Three records in one call, the second failing once: four deliver spans, the failed attempt with
     * {@code error.type}, its retry a span of its own with {@code resend_count} 1, linked to another fetch span; each
     * attempt counted.
     */
    @Test
    void separateAttemptInsideABatch() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().shards(1).headFailuresBeforeDivert(1);
        var telemetry = telemetry(config);
        var handler = new Handler();
        handler.fail = r -> r.keyAsString().equals("b") && r.attempts() == 0
                ? Outcome.retry(new IOException("down once")) : null;
        var runtime = start(config, telemetry, handler);
        var ids = transactions.inTx(c -> runtime.outbox().sendAll(Stream.of("a", "b", "c")
                .map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));
        var b = ids.get(1);
        var dest = DESTINATION.getKey();

        // a step whose link target has not ended yet is still committing
        await(() -> telemetry.meters().count(DELIVERED, dest, topic) == 3 && delivers().size() == 4
                && delivers().stream().flatMap(d -> d.getLinks().stream()).allMatch(l -> !spans(
                        s -> s.getSpanId().equals(l.getSpanContext().getSpanId())).isEmpty()));

        assertThat(handler.calls.get(0)).as("the first call").containsExactlyElementsOf(ids);
        assertFetchSpansHaveRecords();
        var first = span(s -> s.getName().startsWith("fetch ") && linkedFrom(s, ids.get(0)));
        assertThat(first.getAttributes().get(BATCH_COUNT)).as("the first step's records").isEqualTo(3);
        for (var id : List.of(ids.get(0), ids.get(2))) {
            assertThat(deliver(id).getLinks()).extracting(l -> l.getSpanContext().getSpanId())
                    .containsExactly(first.getSpanId());
        }
        var attempts = delivers().stream().filter(s -> b.toString().equals(s.getAttributes().get(RECORD_ID)))
                .sorted(Comparator.comparingLong(SpanData::getStartEpochNanos)).toList();
        assertThat(attempts).hasSize(2);
        var failed = attempts.get(0);
        var retried = attempts.get(1);
        assertThat(failed.getAttributes().get(ERROR_TYPE)).isEqualTo(IOException.class.getName());
        assertThat(failed.getAttributes().get(RESEND_COUNT)).isNull();
        assertThat(failed.getLinks()).extracting(l -> l.getSpanContext().getSpanId())
                .containsExactly(first.getSpanId());
        assertThat(retried.getAttributes().get(ERROR_TYPE)).isNull();
        assertThat(retried.getAttributes().get(RESEND_COUNT)).isEqualTo(1);
        assertThat(retried.getLinks()).singleElement().satisfies(l -> {
            assertThat(l.getSpanContext().getSpanId()).isNotEqualTo(first.getSpanId());
            var retryFetch = span(s -> s.getSpanId().equals(l.getSpanContext().getSpanId()));
            assertThat(retryFetch.getName()).isEqualTo("fetch " + config.table());
            assertThat(retryFetch.getAttributes().get(BATCH_COUNT)).as("the retry step's records").isEqualTo(1);
        });
        var enqueue = enqueue(b);
        assertThat(attempts).allSatisfy(s -> assertThat(s.getParentSpanId()).isEqualTo(enqueue.getSpanId()));
        assertThat(telemetry.meters().count(DELIVERED, dest, topic, ERROR_TYPE.getKey(), IOException.class.getName()))
                .isEqualTo(1);
    }

    /**
     * A read of exactly {@code batchSize} rows cannot tell whether more follow, so the relay steps the shard again and
     * that step reads nothing: it records 0 in the fetch batch-size histogram and exports neither a fetch nor a settle
     * span. Each exported fetch span has records and one settle child.
     */
    @Test
    void emptyStepExportsNoSpan() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().shards(1).batchSize(2).relayEnabled(false);
        var telemetry = telemetry(config);
        var runtime = start(config, telemetry, new Handler());
        transactions.inTx(c -> runtime.outbox().sendAll(Stream.of("a", "b")
                .map(k -> OutboxMessage.of(topic, k, bytes(k))).toList()));

        runtime.relay().drain();

        var sizes = telemetry.meters().histogram(FETCH_BATCH_SIZE);
        assertThat(sizes).as("an empty step after the full one").contains(0L);
        var fetches = spans(s -> s.getName().startsWith("fetch "));
        assertThat(fetches).as("a fetch span per step with records").hasSize((int) sizes.stream().filter(n -> n > 0)
                .count());
        assertFetchSpansHaveRecords();
        assertThat(fetches).extracting(s -> s.getAttributes().get(BATCH_COUNT)).containsExactly(2L);
        assertThat(spans(s -> s.getName().startsWith("settle "))).singleElement()
                .satisfies(s -> assertThat(s.getParentSpanId()).isEqualTo(fetches.get(0).getSpanId()));
    }

    /**
     * A step that fails before it has records, here its main read decoding a row whose stored headers are corrupt,
     * still exports its fetch span, with ERROR status, {@code error.type} and 0 records, and no settle span; a failed
     * step records nothing in the fetch batch-size histogram.
     */
    @Test
    void failedStepExportsItsFetchSpan() throws Exception {
        var topic = TestKafka.createTopic(1);
        var config = KafkaGuaranteesIT.config().shards(1);
        var telemetry = telemetry(config);
        var handler = new Handler();
        var runtime = start(config, telemetry, handler);
        // committed corrupt: the relay never sees the row as written
        transactions.inTx(c -> {
            var id = runtime.outbox().send(OutboxMessage.of(topic, "k", bytes("v")));
            try (var ps = c.prepareStatement("update " + config.table() + " set headers = ? where message_id = ?")) {
                ps.setBytes(1, CORRUPT_HEADERS);
                ps.setObject(2, id);
                assertThat(ps.executeUpdate()).isOne();
            }
            return id;
        });

        Predicate<SpanData> failedFetch = s -> s.getName().equals("fetch " + config.table())
                && s.getStatus().getStatusCode() == StatusCode.ERROR;
        await(() -> !spans(failedFetch).isEmpty());

        var fetch = spans(failedFetch).get(0);
        assertThat(fetch.getKind()).isEqualTo(SpanKind.CLIENT);
        assertCommon(fetch, config.table(), "fetch", "receive");
        assertThat(fetch.getAttributes().get(ERROR_TYPE)).isEqualTo(IllegalArgumentException.class.getName());
        assertThat(fetch.getAttributes().get(BATCH_COUNT)).isZero();
        assertThat(spans(s -> s.getName().startsWith("settle ") && s.getParentSpanId().equals(fetch.getSpanId())))
                .as("no settle of a step that never settled").isEmpty();
        assertThat(handler.calls).as("handler calls").isEmpty();
        assertThat(telemetry.meters().histogram(FETCH_BATCH_SIZE)).as("only successful steps, none with records")
                .allMatch(n -> n == 0);
    }

    /**
     * A rolled-back transaction of two sends counts none; a committed one of three sends counts three; a write the
     * database refuses counts once, with its {@code error.type}, and its enqueue span ends in error.
     */
    @Test
    void rolledBackWriteIsNotCounted() throws Exception {
        var topic = TestKafka.createTopic(1);
        var refused = "refused-" + UUID.randomUUID();
        var config = KafkaGuaranteesIT.config().relayEnabled(false);
        var telemetry = telemetry(config);
        var runtime = start(config, telemetry, new Handler());
        refuse(config, refused);
        var outbox = runtime.outbox();

        var rolledBack = new ArrayList<UUID>();
        assertThatThrownBy(() -> transactions.inTx(c -> {
            rolledBack.add(outbox.send(OutboxMessage.of(topic, "r1", bytes("r1"))));
            rolledBack.add(outbox.send(OutboxMessage.of(topic, "r2", bytes("r2"))));
            throw new IllegalStateException("rollback");
        })).hasMessage("rollback");
        var dest = DESTINATION.getKey();
        assertThat(telemetry.meters().count(ENQUEUED, dest, topic)).as("after the rollback").isZero();
        assertThat(rolledBack).allSatisfy(id -> assertThat(enqueue(id).getStatus().getStatusCode())
                .isEqualTo(StatusCode.UNSET));

        transactions.inTx(c -> Stream.of("c1", "c2", "c3")
                .map(k -> outbox.send(OutboxMessage.of(topic, k, bytes(k)))).toList());
        assertThat(telemetry.meters().count(ENQUEUED, dest, topic)).as("after the commit").isEqualTo(3);

        assertThatThrownBy(() -> transactions.inTx(c -> outbox.send(OutboxMessage.of(refused, "k", bytes("k")))))
                .isInstanceOf(OutboxSqlException.class);
        assertThat(telemetry.meters().count(ENQUEUED, dest, refused)).isZero();
        assertThat(telemetry.meters().count(ENQUEUED, dest, refused, ERROR_TYPE.getKey(),
                OutboxSqlException.class.getName())).isEqualTo(1);
        assertThat(span(s -> s.getName().equals("enqueue " + refused))).satisfies(s -> {
            assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(s.getAttributes().get(ERROR_TYPE)).isEqualTo(OutboxSqlException.class.getName());
        });
        assertThat(telemetry.meters().count(ENQUEUED, dest, topic)).as("in the end").isEqualTo(3);
    }

    private static void assertCommon(SpanData span, String table, String operation, String type) {
        assertThat(span.getAttributes().get(SYSTEM)).as(span.getName()).isEqualTo("outbox");
        assertThat(span.getAttributes().get(OPERATION_NAME)).as(span.getName()).isEqualTo(operation);
        assertThat(span.getAttributes().get(OPERATION_TYPE)).as(span.getName()).isEqualTo(type);
        assertThat(span.getAttributes().get(COLLECTION)).as(span.getName()).isEqualTo(table);
    }

    /**
     * {@link KafkaOutboxSender#handler()} that keeps each call's message ids and every record it got: a record
     * {@link #fail} answers for gets that outcome instead of being sent.
     */
    private final class Handler implements OutboxHandler {

        private final KafkaOutboxHandler kafka = sender().handler();
        final List<List<UUID>> calls = new CopyOnWriteArrayList<>();
        final List<OutboxRecord> records = new CopyOnWriteArrayList<>();
        volatile Function<OutboxRecord, Outcome> fail = r -> null; // null: send it

        @Override
        public void handle(OutboxRecord record) {
            kafka.handle(record);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> batch) {
            calls.add(batch.stream().map(OutboxRecord::messageId).toList());
            records.addAll(batch);
            var outcomes = new Outcome[batch.size()];
            var sending = new ArrayList<OutboxRecord>();
            for (int i = 0; i < outcomes.length; i++) {
                outcomes[i] = fail.apply(batch.get(i));
                if (outcomes[i] == null) {
                    sending.add(batch.get(i));
                } else {
                    // the handler's own word, as a sender reports each record it knows the outcome of
                    OutboxHandler.recordFinished(batch.get(i), outcomes[i]);
                }
            }
            var sent = (sending.isEmpty() ? List.<Outcome>of() : kafka.handleBatch(sending)).iterator();
            for (int i = 0; i < outcomes.length; i++) {
                if (outcomes[i] == null) {
                    outcomes[i] = sent.next();
                }
            }
            return Arrays.asList(outcomes);
        }
    }

    /**
     * A sender whose producers start a {@code send <topic>} PRODUCER span per record, a child of the current context,
     * write it into the record's headers (W3C, replacing any header of the same name) and end it at the ack.
     */
    private KafkaOutboxSender sender() {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafka.bootstrapServers());
        var sender = new KafkaOutboxSender(() -> properties, props -> new KafkaProducer<byte[], byte[]>(props) {
            @Override
            public Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record, @Nullable Callback callback) {
                var span = tracer.spanBuilder("send " + record.topic()).setSpanKind(SpanKind.PRODUCER)
                        .setParent(Context.current()).setAttribute("messaging.system", "kafka").startSpan();
                var context = Context.current().with(span);
                propagator.inject(context, record.headers(), MessagingConventionsIT::setHeader);
                try (var scope = context.makeCurrent()) {
                    return super.send(record, (metadata, error) -> {
                        if (error != null) {
                            span.setStatus(StatusCode.ERROR).recordException(error);
                        }
                        span.end();
                        if (callback != null) {
                            callback.onCompletion(metadata, error);
                        }
                    });
                } catch (RuntimeException e) {
                    span.setStatus(StatusCode.ERROR).recordException(e);
                    span.end();
                    throw e;
                }
            }
        });
        closing.add(sender);
        return sender;
    }

    private static void setHeader(@Nullable Headers headers, String key, String value) {
        if (headers != null) {
            headers.remove(key);
            headers.add(key, value.getBytes(UTF_8));
        }
    }

    private MessagingConventionsTelemetry telemetry(TestOutboxConfig config) {
        return new MessagingConventionsTelemetry(tracer, propagator, config.table());
    }

    private OutboxRuntime start(TestOutboxConfig config, MessagingConventionsTelemetry telemetry, Handler handler) {
        return start(OutboxRuntime.builder(config, transactions, handler).telemetry(telemetry));
    }

    private OutboxRuntime start(OutboxRuntime.Builder builder) {
        var runtime = builder.build();
        closing.add(runtime);
        runtime.start();
        return runtime;
    }

    /** A {@code BEFORE INSERT} trigger on the outbox table that raises for {@code topic}. */
    private void refuse(TestOutboxConfig config, String topic) throws Exception {
        try (var c = transactions.newConnection()) {
            var function = config.table() + "_refuse";
            TestSql.execute(c, "create function " + function + "() returns trigger language plpgsql as $$ begin"
                    + " if new.topic = '" + topic + "' then raise exception 'refused'; end if; return new; end $$");
            TestSql.execute(c, "create trigger refuse before insert on " + config.table()
                    + " for each row execute function " + function + "()");
        }
    }

    private List<SpanData> spans(Predicate<SpanData> filter) {
        return spans.getFinishedSpanItems().stream().filter(filter).toList();
    }

    /** The one finished span matching {@code filter}. */
    private SpanData span(Predicate<SpanData> filter) {
        var found = spans(filter);
        assertThat(found).hasSize(1);
        return found.get(0);
    }

    private SpanData enqueue(UUID id) {
        return span(s -> s.getName().startsWith("enqueue ") && id.toString().equals(s.getAttributes().get(RECORD_ID)));
    }

    /** The one deliver span of {@code id}. */
    private SpanData deliver(UUID id) {
        return span(s -> s.getName().startsWith("deliver ") && id.toString().equals(s.getAttributes().get(RECORD_ID)));
    }

    /** The one fetch span of {@code table}. */
    private SpanData fetch(String table) {
        return span(s -> s.getName().equals("fetch " + table));
    }

    /** A step without records exports no fetch span: every one exported counts at least one record. */
    private void assertFetchSpansHaveRecords() {
        assertThat(spans(s -> s.getName().startsWith("fetch "))).isNotEmpty().allSatisfy(s -> assertThat(
                s.getAttributes().get(BATCH_COUNT)).as("records of %s", s.getSpanId()).isGreaterThanOrEqualTo(1));
    }

    private List<SpanData> delivers() {
        return spans(s -> s.getName().startsWith("deliver "));
    }

    /** Whether the deliver span of {@code id} links to {@code fetch}. */
    private boolean linkedFrom(SpanData fetch, UUID id) {
        return deliver(id).getLinks().stream().map(LinkData::getSpanContext)
                .anyMatch(c -> c.getSpanId().equals(fetch.getSpanId()));
    }

    @Nullable
    private static String traceparent(OutboxRecord record) {
        String last = null;
        for (var h : record.headers()) {
            if (h.name().equals("traceparent") && h.value() != null) {
                last = new String(h.value(), UTF_8);
            }
        }
        return last;
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
