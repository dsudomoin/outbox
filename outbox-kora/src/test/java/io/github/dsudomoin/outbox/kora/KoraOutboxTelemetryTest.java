package io.github.dsudomoin.outbox.kora;

import static io.github.dsudomoin.outbox.testing.TestTracing.SPAN_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACEPARENT;
import static io.github.dsudomoin.outbox.testing.TestTracing.TRACE_ID;
import static io.github.dsudomoin.outbox.testing.TestTracing.header;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.GuardedTelemetry;
import io.github.dsudomoin.outbox.internal.RecordScope;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.github.dsudomoin.outbox.testing.TestTracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import ru.tinkoff.kora.common.Context;
import ru.tinkoff.kora.opentelemetry.common.OpentelemetryContext;

class KoraOutboxTelemetryTest {

    private static final Instant NOW = Instant.parse("2026-10-27T12:00:00Z");

    @AfterEach
    void clear() {
        Context.clear();
    }

    @Test
    void recordScopeCarriesTraceWithoutTracer() throws Exception {
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), null);
        var record = record(List.of(header("traceparent", TRACEPARENT)));
        var delivery = telemetry.deliveryStarted(List.of(record), NOW);

        assertThat(delivery.handed(List.of(record))).containsExactly(record);
        var traceId = delivery.inRecordScope(record,
                () -> OpentelemetryContext.getSpan().getSpanContext().getTraceId());
        assertThat(traceId).isEqualTo(TRACE_ID);
        assertThat(OpentelemetryContext.getSpan()).as("the caller's context is restored").isNull();
    }

    @Test
    void metricsWithoutTracer() {
        var registry = new SimpleMeterRegistry();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(registry), null);

        telemetry.enqueued("orders");
        telemetry.delivered("orders", Duration.ofMillis(10));
        var delivery = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(registry.get("outbox.messages.enqueued").tag("topic", "orders").counter().count()).isEqualTo(1);
        assertThat(registry.get("outbox.messages.delivered").tag("topic", "orders").counter().count()).isEqualTo(1);
    }

    @Test
    void spansWithoutRegistry() {
        var spans = InMemorySpanExporter.create();
        var tracer = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build()
                .get("test");
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer);
        var record = record(List.of(header("traceparent", TRACEPARENT)));

        var delivery = telemetry.deliveryStarted(List.of(record), NOW);
        var handed = delivery.handed(List.of(record));
        delivery.finished(List.of(Outcome.delivered()));

        var span = spans.getFinishedSpanItems().get(0);
        assertThat(span.getName()).isEqualTo("outbox deliver orders");
        assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
        assertThat(span.getParentSpanId()).isEqualTo(SPAN_ID);
        assertThat(span.getAttributes().get(AttributeKey.stringKey("messaging.destination.name"))).isEqualTo("orders");
        assertThat(span.getAttributes().get(AttributeKey.stringKey("outbox.outcome"))).isEqualTo("delivered");
        assertThat(span.getAttributes().get(AttributeKey.longKey("outbox.attempt"))).isEqualTo(1L);
        assertThat(traceparent(handed.get(0))).as("the handed record carries the delivery span")
                .isEqualTo("00-" + TRACE_ID + "-" + span.getSpanId() + "-01");
        assertThat(traceparent(record)).as("the stored record keeps its own").isEqualTo(TRACEPARENT);
    }

    @Test
    void aRejectedContractIsCounted() {
        var registry = new SimpleMeterRegistry();

        new KoraOutboxTelemetry(new OutboxMetrics(registry), null).contractRejected("orders", "Created", 2);

        assertThat(registry.get("outbox.messages.contract_rejected").tags("topic", "orders", "type", "Created",
                "version", "2").counter().count()).isEqualTo(1);
    }

    @Test
    void aRejectedAndAnUncheckedRouteAreCounted() {
        var registry = new SimpleMeterRegistry();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(registry), null);

        telemetry.routeRejected("orders");
        telemetry.routeUnchecked("audit");

        assertThat(registry.get("outbox.messages.route_rejected").tag("topic", "orders").counter().count())
                .isEqualTo(1);
        assertThat(registry.get("outbox.messages.route_unchecked").tag("topic", "audit").counter().count())
                .isEqualTo(1);
    }

    @Test
    void aWaitingRecordEndsItsSpanAsWaitingWithoutAnErrorAndIsCounted() {
        var registry = new SimpleMeterRegistry();
        var spans = InMemorySpanExporter.create();
        var tracer = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build()
                .get("test");
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(registry), tracer);

        var delivery = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        delivery.finished(List.of(Outcome.waitFor("v3 is not readable here")));
        telemetry.waited("orders", Outcome.Waiting.Kind.HANDLER);

        var span = spans.getFinishedSpanItems().get(0);
        assertThat(span.getAttributes().get(AttributeKey.stringKey("outbox.outcome"))).isEqualTo("waiting");
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        assertThat(span.getEvents()).isEmpty();
        assertThat(registry.get("outbox.messages.waiting").tags("topic", "orders", "kind", "handler").counter()
                .count()).isEqualTo(1);
    }

    @Test
    void eachRecordOfACallKeepsItsOwnTrace() {
        var spans = InMemorySpanExporter.create();
        var tracer = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build()
                .get("test");
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer);
        var writers = List.of(tracer.spanBuilder("writer 1").startSpan(), tracer.spanBuilder("writer 2").startSpan());
        writers.forEach(Span::end);
        var records = writers.stream().map(w -> record(List.of(header("traceparent",
                "00-" + w.getSpanContext().getTraceId() + "-" + w.getSpanContext().getSpanId() + "-01")))).toList();

        var delivery = telemetry.deliveryStarted(records, NOW);
        delivery.handed(records);
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));

        for (int i = 0; i < records.size(); i++) {
            var id = records.get(i).messageId().toString();
            var writer = writers.get(i).getSpanContext();
            assertThat(spans.getFinishedSpanItems())
                    .filteredOn(s -> id.equals(s.getAttributes().get(AttributeKey.stringKey("outbox.message_id"))))
                    .singleElement()
                    .satisfies(span -> {
                        assertThat(span.getTraceId()).isEqualTo(writer.getTraceId());
                        assertThat(span.getParentSpanId()).isEqualTo(writer.getSpanId());
                    });
        }
    }

    @Test
    void relayFailuresReachTheCounter() {
        var registry = new SimpleMeterRegistry();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(registry), null);

        telemetry.relayFailed(new SQLException("x"));

        assertThat(registry.get("outbox.relay.failures").counter().count()).isEqualTo(1);
    }

    @Test
    void enqueueSpanPerMessageIsTheStoredParent() throws Exception {
        var spans = InMemorySpanExporter.create();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer(spans));
        TestTracing.activateSpan();
        var writer = Context.current();
        var enqueue = telemetry.enqueueStarted();
        var id = UUID.randomUUID();
        var own = List.of(header("traceparent", "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01"));

        var first = enqueue.message(OutboxMessage.of("orders", "k", null), id, 3);
        var stored = first.headers(enqueue.headers(List.of()));
        var inside = first.inScope(() -> OpentelemetryContext.getSpan().getSpanContext().getSpanId());
        assertThat(Context.current()).as("the writer's context is current again").isSameAs(writer);
        first.inserted();
        var second = enqueue.message(new OutboxMessage("orders", null, null, own), UUID.randomUUID(), 1);
        assertThat(second.headers(enqueue.headers(own))).as("a message with its own trace keeps it").isEqualTo(own);
        second.inserted();
        var third = enqueue.message(OutboxMessage.of("audit", (byte[]) null), UUID.randomUUID(), 0);
        third.failed(new SQLException("insert failed"));

        var finished = spans.getFinishedSpanItems();
        assertThat(finished).hasSize(3);
        var span = finished.get(0);
        assertThat(span.getName()).isEqualTo("outbox enqueue orders");
        assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
        assertThat(span.getParentSpanId()).as("the writer's span").isEqualTo(SPAN_ID);
        assertThat(span.getAttributes().get(AttributeKey.stringKey("messaging.system"))).isEqualTo("outbox");
        assertThat(span.getAttributes().get(AttributeKey.stringKey("messaging.destination.name")))
                .isEqualTo("orders");
        assertThat(span.getAttributes().get(AttributeKey.stringKey("outbox.message_id"))).isEqualTo(id.toString());
        assertThat(span.getAttributes().get(AttributeKey.longKey("outbox.shard"))).isEqualTo(3L);
        assertThat(stored).as("the message stores the enqueue span")
                .containsExactly(header("traceparent", "00-" + TRACE_ID + "-" + span.getSpanId() + "-01"));
        assertThat(inside).as("the insert runs with the enqueue span current").isEqualTo(span.getSpanId());
        assertThat(finished.get(1).getParentSpanId()).isEqualTo(SPAN_ID);
        var failed = finished.get(2);
        assertThat(failed.getName()).isEqualTo("outbox enqueue audit");
        assertThat(failed.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(failed.getEvents()).extracting(EventData::getName).containsExactly("exception");
    }

    @Test
    void stepAndSettleSpansAreClient() {
        var spans = InMemorySpanExporter.create();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer(spans));
        var before = Context.current();

        var step = telemetry.stepStarted(List.of(2));
        var stepSpan = OpentelemetryContext.getSpan().getSpanContext();
        assertThat(Context.current()).as("a forked context").isNotSameAs(before);
        step.fetched(2, 5);
        var settle = step.settleStarted();
        var settleSpan = OpentelemetryContext.getSpan().getSpanContext();
        settle.committed(4);
        assertThat(OpentelemetryContext.getSpan().getSpanContext()).as("the step's again").isEqualTo(stepSpan);
        step.finished();

        assertThat(Context.current()).as("the thread's context is restored").isSameAs(before);
        assertThat(OpentelemetryContext.getSpan()).isNull();
        var finished = spans.getFinishedSpanItems();
        assertThat(finished).extracting(SpanData::getName).containsExactly("outbox relay settle", "outbox relay step");
        var settled = finished.get(0);
        var stepped = finished.get(1);
        assertThat(stepped.getSpanContext()).isEqualTo(stepSpan);
        assertThat(stepped.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(stepped.getParentSpanContext().isValid()).isFalse();
        assertThat(stepped.getAttributes().get(AttributeKey.stringKey("messaging.system"))).isEqualTo("outbox");
        assertThat(stepped.getAttributes().get(AttributeKey.stringKey("outbox.shards"))).isEqualTo("2");
        assertThat(stepped.getAttributes().get(AttributeKey.longKey("outbox.shard"))).isEqualTo(2L);
        assertThat(stepped.getAttributes().get(AttributeKey.longKey("outbox.records"))).isEqualTo(5L);
        assertThat(stepped.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        assertThat(settled.getSpanContext()).isEqualTo(settleSpan);
        assertThat(settled.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(settled.getParentSpanId()).isEqualTo(stepSpan.getSpanId());
        assertThat(settled.getAttributes().get(AttributeKey.stringKey("messaging.system"))).isEqualTo("outbox");
        assertThat(settled.getAttributes().get(AttributeKey.longKey("outbox.records"))).isEqualTo(4L);
    }

    @Test
    void aFailedStepEndsBothSpansFailedAndRestoresTheContext() {
        var spans = InMemorySpanExporter.create();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer(spans));
        TestTracing.activateSpan(); // a drain() caller's span parents the step
        var before = Context.current();

        var step = telemetry.stepStarted(List.of(1, 3));
        step.fetched(1, 2);
        step.fetched(3, 1);
        var settle = step.settleStarted();
        var error = new SQLException("commit failed");
        settle.failed(3, error);
        step.failed(error);

        assertThat(Context.current()).isSameAs(before);
        assertThat(OpentelemetryContext.getSpan().getSpanContext().getSpanId()).isEqualTo(SPAN_ID);
        var finished = spans.getFinishedSpanItems();
        assertThat(finished).hasSize(2).allSatisfy(s -> {
            assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
            assertThat(s.getEvents()).extracting(EventData::getName).containsExactly("exception");
        });
        var stepped = finished.get(1);
        assertThat(stepped.getParentSpanId()).isEqualTo(SPAN_ID);
        assertThat(stepped.getAttributes().get(AttributeKey.stringKey("outbox.shards"))).isEqualTo("1,3");
        assertThat(stepped.getAttributes().get(AttributeKey.longKey("outbox.shard"))).isNull();
        assertThat(stepped.getAttributes().get(AttributeKey.longKey("outbox.records"))).isEqualTo(3L);
        assertThat(finished.get(0).getAttributes().get(AttributeKey.longKey("outbox.records"))).isEqualTo(3L);
    }

    @Test
    void aSettleThatFailsToStartLeavesTheStepCurrentAndTheStepStillRestoresTheContext() {
        var spans = InMemorySpanExporter.create();
        var sdk = tracer(spans);
        Tracer tracer = name -> {
            if (name.equals("outbox relay settle")) {
                throw new IllegalStateException("tracer failure (test)");
            }
            return sdk.spanBuilder(name);
        };
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer);
        var before = Context.current();

        var step = telemetry.stepStarted(List.of(0));
        var stepContext = Context.current();
        assertThatThrownBy(step::settleStarted).hasMessageContaining("tracer failure");
        assertThat(Context.current()).isSameAs(stepContext);
        step.finished();
        step.failed(new SQLException("a second end (test)"));

        assertThat(Context.current()).isSameAs(before);
        assertThat(spans.getFinishedSpanItems()).singleElement()
                .satisfies(s -> assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET));
    }

    @Test
    void aSettleLeftOpenEndsWithItsStep() {
        var spans = InMemorySpanExporter.create();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer(spans));
        var before = Context.current();

        var step = telemetry.stepStarted(List.of(0));
        var settle = step.settleStarted();
        step.failed(new SQLException("x"));
        assertThat(Context.current()).isSameAs(before);
        settle.committed(1); // too late: ignored, the thread's context stays

        assertThat(Context.current()).isSameAs(before);
        assertThat(spans.getFinishedSpanItems()).extracting(SpanData::getName)
                .containsExactly("outbox relay settle", "outbox relay step");
    }

    @Test
    void deliverIsConsumerWithALinkToTheStep() throws Exception {
        var spans = InMemorySpanExporter.create();
        var tracer = tracer(spans);
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer);
        var writers = List.of(tracer.spanBuilder("writer 1").startSpan(), tracer.spanBuilder("writer 2").startSpan(),
                tracer.spanBuilder("writer 3").startSpan());
        writers.forEach(Span::end);
        var records = writers.stream().map(w -> record(List.of(header("traceparent",
                "00-" + w.getSpanContext().getTraceId() + "-" + w.getSpanContext().getSpanId() + "-01")), 7)).toList();

        var step = telemetry.stepStarted(List.of(7));
        var stepSpan = OpentelemetryContext.getSpan().getSpanContext();
        var delivery = telemetry.deliveryStarted(step, records, NOW);
        var handed = delivery.handed(records);
        var inScope = delivery.inRecordScope(handed.get(0),
                () -> OpentelemetryContext.getSpan().getSpanContext().getSpanId());
        assertThat(OpentelemetryContext.getSpan().getSpanContext()).as("the step's again").isEqualTo(stepSpan);
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered(), Outcome.delivered()));
        step.finished();

        for (int i = 0; i < records.size(); i++) {
            var id = records.get(i).messageId().toString();
            var writer = writers.get(i).getSpanContext();
            var span = spans.getFinishedSpanItems().stream()
                    .filter(s -> id.equals(s.getAttributes().get(AttributeKey.stringKey("outbox.message_id"))))
                    .findFirst().orElseThrow();
            assertThat(span.getName()).isEqualTo("outbox deliver orders");
            assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
            assertThat(span.getTraceId()).isEqualTo(writer.getTraceId());
            assertThat(span.getParentSpanId()).isEqualTo(writer.getSpanId());
            assertThat(span.getLinks()).extracting(LinkData::getSpanContext).containsExactly(stepSpan);
            assertThat(span.getAttributes().get(AttributeKey.stringKey("messaging.system"))).isEqualTo("outbox");
            assertThat(span.getAttributes().get(AttributeKey.longKey("outbox.shard"))).isEqualTo(7L);
            if (i == 0) {
                assertThat(inScope).isEqualTo(span.getSpanId());
            }
        }
    }

    @Test
    void withoutStoredContextTheStepIsTheParent() {
        var spans = InMemorySpanExporter.create();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer(spans));
        var none = record(List.of());
        var malformed = record(List.of(header("traceparent", TRACEPARENT), header("traceparent", "00-zz-01")));

        var step = telemetry.stepStarted(List.of(0));
        var stepSpan = OpentelemetryContext.getSpan().getSpanContext();
        var delivery = telemetry.deliveryStarted(step, List.of(none, malformed), NOW);
        var handed = delivery.handed(List.of(none, malformed));
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));
        step.finished();

        var delivered = spans.getFinishedSpanItems().stream().filter(s -> s.getName().startsWith("outbox deliver"))
                .toList();
        assertThat(delivered).hasSize(2).allSatisfy(span -> {
            assertThat(span.getTraceId()).isEqualTo(stepSpan.getTraceId());
            assertThat(span.getParentSpanId()).isEqualTo(stepSpan.getSpanId());
            assertThat(span.getLinks()).as("the step is its parent: no link").isEmpty();
        });
        assertThat(traceparent(handed.get(0))).isEqualTo(
                "00-" + stepSpan.getTraceId() + "-" + delivered.get(0).getSpanId() + "-01");
    }

    @Test
    void deliverEndsAtRecordFinished() {
        var spans = InMemorySpanExporter.create();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer(spans));
        var records = List.of(record(List.of()), record(List.of()));

        var delivery = telemetry.deliveryStarted(OutboxTelemetry.RelayStep.NONE, records, NOW);
        var handed = delivery.handed(records);
        delivery.recordFinished(handed.get(0), Outcome.permanent(new IllegalArgumentException("bad payload")));
        assertThat(spans.getFinishedSpanItems()).as("ended at the report").hasSize(1);
        delivery.recordFinished(handed.get(0), Outcome.delivered()); // reported twice: the first wins
        delivery.recordFinished(record(List.of()), Outcome.delivered()); // not of this call: ignored
        delivery.recordFinished(records.get(1), Outcome.requeue()); // the stored record counts as well
        delivery.finished(List.of(Outcome.retry(new IllegalStateException("timed out")), Outcome.delivered()));
        delivery.failed(new InterruptedException("late"));

        var finished = spans.getFinishedSpanItems();
        assertThat(finished).hasSize(2);
        assertThat(finished.get(0).getAttributes().get(AttributeKey.stringKey("outbox.outcome")))
                .isEqualTo("permanent");
        assertThat(finished.get(0).getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(finished.get(1).getAttributes().get(AttributeKey.stringKey("outbox.outcome")))
                .isEqualTo("requeued");
        assertThat(finished.get(1).getParentSpanContext().isValid()).as("no stored trace, no step: a root").isFalse();
    }

    @Test
    void reportsRacingTheCallsEndEndEachSpanOnce() throws Exception {
        var spans = InMemorySpanExporter.create();
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), tracer(spans));
        int n = 500;
        var records = new ArrayList<OutboxRecord>();
        for (int i = 0; i < n; i++) {
            records.add(record(List.of()));
        }
        var outcomes = Collections.nCopies(n, Outcome.delivered());

        var delivery = telemetry.deliveryStarted(OutboxTelemetry.RelayStep.NONE, records, NOW);
        var handed = delivery.handed(records);
        var start = new CountDownLatch(1);
        var handler = new Thread(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            for (var r : handed) {
                delivery.recordFinished(r, Outcome.requeue());
            }
        });
        handler.start();
        start.countDown();
        delivery.finished(outcomes);
        handler.join();

        assertThat(spans.getFinishedSpanItems()).hasSize(n)
                .extracting(s -> s.getAttributes().get(AttributeKey.stringKey("outbox.message_id")))
                .doesNotHaveDuplicates();
    }

    @Test
    void withoutATracerNothingChanges() throws Exception {
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), null);
        TestTracing.activateSpan();
        var writer = Context.current();

        var enqueue = telemetry.enqueueStarted();
        var message = enqueue.message(OutboxMessage.of("orders", "k", null), UUID.randomUUID(), 0);
        assertThat(message).isSameAs(OutboxTelemetry.Message.NONE);
        assertThat(message.headers(enqueue.headers(List.of())))
                .as("the writer's trace is stored").containsExactly(header("traceparent", TRACEPARENT));
        Context.clear();
        var before = Context.current();
        var step = telemetry.stepStarted(List.of(0));
        assertThat(step).isSameAs(OutboxTelemetry.RelayStep.NONE);
        assertThat(Context.current()).isSameAs(before);
        var record = record(List.of(header("traceparent", TRACEPARENT)));
        var delivery = telemetry.deliveryStarted(step, List.of(record), NOW);
        assertThat(delivery.handed(List.of(record))).containsExactly(record);
        var traceId = delivery.inRecordScope(record,
                () -> OpentelemetryContext.getSpan().getSpanContext().getTraceId());
        assertThat(traceId).as("handled in a Kora context of its stored trace").isEqualTo(TRACE_ID);
        assertThat(Context.current()).isSameAs(before);
        assertThat(writer).isNotSameAs(before);
    }

    /**
     * As before the per-record reports: a sender that only reports outcomes early (Kafka's) enters no record scope per
     * send, through the core's guard too, while the handler's own record scope still carries the stored trace.
     */
    @Test
    void withoutATracerASenderHearsNoReports() throws Exception {
        var telemetry = GuardedTelemetry.of(new KoraOutboxTelemetry(new OutboxMetrics(null), null));
        var record = record(List.of(header("traceparent", TRACEPARENT)));
        var delivery = telemetry.deliveryStarted(telemetry.stepStarted(List.of(0)), List.of(record), NOW);

        assertThat(RecordScope.bound(delivery, RecordScope::reports)).as("reports heard").isFalse();
        var traceId = RecordScope.bound(delivery, () -> RecordScope.run(record,
                () -> OpentelemetryContext.getSpan().getSpanContext().getTraceId()));
        assertThat(traceId).isEqualTo(TRACE_ID);
    }

    @Test
    void enqueueAddsCurrentTraceUnlessTheMessageHasOne() {
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), null);
        TestTracing.activateSpan();
        var enqueue = telemetry.enqueueStarted();

        assertThat(enqueue.headers(List.of())).containsExactly(header("traceparent", TRACEPARENT));
        var own = List.of(header("traceparent", "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01"));
        assertThat(enqueue.headers(own)).isSameAs(own);
    }

    @Test
    void releaseThreadClearsTheKoraContext() {
        var telemetry = new KoraOutboxTelemetry(new OutboxMetrics(null), null);
        TestTracing.activateSpan();

        telemetry.releaseThread();

        assertThat(OpentelemetryContext.getSpan()).isNull();
    }

    private static OutboxRecord record(List<OutboxHeader> headers) {
        return new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, headers, NOW.minusSeconds(1), 0, 0);
    }

    private static OutboxRecord record(List<OutboxHeader> headers, int shard) {
        return new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, headers, NOW.minusSeconds(1), 0, 0,
                shard);
    }

    private static Tracer tracer(InMemorySpanExporter spans) {
        return SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build().get("test");
    }

    private static String traceparent(OutboxRecord record) {
        return record.headers().stream().filter(h -> h.name().equals("traceparent"))
                .map(h -> new String(h.value(), UTF_8)).findFirst().orElseThrow();
    }
}
