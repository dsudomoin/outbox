package io.github.dsudomoin.outbox.micrometer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.GuardedTelemetry;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
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
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ObservationOutboxTelemetryTest {

    private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String SPAN_ID = "b7ad6b7169203331";
    private static final String TRACEPARENT = "00-" + TRACE_ID + "-" + SPAN_ID + "-01";
    private static final String UNSAMPLED = "00-" + TRACE_ID + "-" + SPAN_ID + "-00";
    private static final Instant NOW = Instant.parse("2026-10-27T12:00:00Z");

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
    private final ObservationRegistry observations = ObservationRegistry.create();

    @BeforeEach
    void handlers() {
        traced(observations).observationConfig().observationHandler(new DefaultMeterObservationHandler(meters));
    }

    /** The tracing handlers, registered first as Spring Boot groups them. */
    private ObservationRegistry traced(ObservationRegistry registry) {
        registry.observationConfig()
                .observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                        new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                        new OutboxDeliveryTracingObservationHandler(tracer, propagator),
                        new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
                        new DefaultTracingObservationHandler(tracer)));
        return registry;
    }

    @AfterEach
    void close() {
        observations.setCurrentObservationScope(null); // thread-local and shared by registries: no leak between tests
        provider.close();
    }

    @Test
    void enqueueContextLandsInHeaders() {
        var parent = tracer.nextSpan().name("parent").start();
        var id = UUID.randomUUID();
        OutboxTelemetry.Message message;
        List<OutboxHeader> headers;
        try (var scope = tracer.withSpan(parent)) {
            var enqueue = telemetry().enqueueStarted();
            message = enqueue.message(OutboxMessage.of("orders", "k", bytes("p")), id, 3);
            headers = message.headers(List.of());
            message.inserted();
            enqueue.finished();
        }
        parent.end();

        var span = finished("outbox enqueue orders");
        assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
        assertThat(span.getParentSpanId()).isEqualTo(parent.context().spanId());
        assertThat(attribute(span, "messaging.system")).isEqualTo("outbox");
        assertThat(attribute(span, "messaging.destination.name")).isEqualTo("orders");
        assertThat(attribute(span, "outbox.message_id")).isEqualTo(id.toString());
        assertThat(attribute(span, "outbox.shard")).isEqualTo("3");
        // its own flags: newer OpenTelemetry sets the random trace id flag besides the sampled one
        assertThat(traceparent(headers)).isEqualTo("00-" + span.getTraceId() + "-" + span.getSpanId() + "-"
                + span.getSpanContext().getTraceFlags().asHex());
    }

    /**
     * The call starts nothing; each message is observed on its own, current while its insert runs, and its headers
     * carry its own span.
     */
    @Test
    void eachMessageIsObservedOnItsOwn() throws Exception {
        var telemetry = telemetry();
        var empty = telemetry.enqueueStarted();
        empty.finished();
        assertThat(meters.find("outbox.enqueue").timer()).as("a call alone").isNull();

        var enqueue = telemetry.enqueueStarted();
        var a = enqueue.message(OutboxMessage.of("orders", "a", bytes("a")), UUID.randomUUID(), 0);
        var b = enqueue.message(OutboxMessage.of("orders", "b", bytes("b")), UUID.randomUUID(), 1);
        var headersA = a.headers(List.of());
        var headersB = b.headers(List.of());
        var inside = a.inScope(() -> tracer.currentSpan().context().spanId());
        assertThat(tracer.currentSpan()).as("the scope is closed after the insert").isNull();
        a.inserted();
        b.failed(new SQLException("x"));
        enqueue.failed(new SQLException("x"));

        var spanA = span(s -> s.getName().equals("outbox enqueue orders")
                && "0".equals(attribute(s, "outbox.shard")));
        var spanB = span(s -> s.getName().equals("outbox enqueue orders")
                && "1".equals(attribute(s, "outbox.shard")));
        assertThat(inside).isEqualTo(spanA.getSpanId());
        assertThat(traceparent(headersA)).contains(spanA.getSpanId());
        assertThat(traceparent(headersB)).contains(spanB.getSpanId());
        assertThat(spanA.getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
        assertThat(spanB.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(meters.get("outbox.enqueue").timers()).extracting(t -> t.count()).containsOnly(1L);
    }

    @Test
    void messageWithTraceContextKeepsIt() {
        var message = telemetry().enqueueStarted()
                .message(OutboxMessage.of("orders", "k", bytes("p")), UUID.randomUUID(), 0);
        var own = List.of(header("traceparent", "00-" + "1".repeat(32) + "-" + "2".repeat(16) + "-01"));

        assertThat(message.headers(own)).isSameAs(own);
        message.inserted();
    }

    /**
     * A message with only its own baggage has no parent of its own: it gets the injected trace headers, keeps its
     * baggage, and stores no second header of that name.
     */
    @Test
    void aMessageWithOnlyItsOwnBaggageGetsTheTraceHeaders() {
        var registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                new PropagatingSenderTracingObservationHandler<>(tracer, withBaggage()),
                new DefaultTracingObservationHandler(tracer)));
        var telemetry = ObservationOutboxTelemetry.builder(registry, new OutboxMetrics(meters)).build();
        var own = List.of(header("a", "1"), header("baggage", "tenant=mine"));
        List<OutboxHeader> stored;
        List<OutboxHeader> bare;
        try (var baggage = io.opentelemetry.api.baggage.Baggage.builder().put("tenant", "acme").build().makeCurrent()) {
            var enqueue = telemetry.enqueueStarted();
            var mine = enqueue.message(OutboxMessage.of("orders", "k", bytes("p")), UUID.randomUUID(), 0);
            var plain = enqueue.message(OutboxMessage.of("orders", "j", bytes("p")), UUID.randomUUID(), 0);
            stored = mine.headers(own);
            bare = plain.headers(List.of());
            mine.inserted();
            plain.inserted();
        }

        assertThat(bare).extracting(OutboxHeader::name).as("the injected headers").contains("traceparent", "baggage");
        assertThat(stored).extracting(OutboxHeader::name).containsOnlyOnce("a", "baggage", "traceparent");
        assertThat(stored).filteredOn(h -> h.name().equals("baggage")).extracting(h -> new String(h.value(), UTF_8))
                .containsExactly("tenant=mine");
        var mineSpan = span(s -> s.getName().equals("outbox enqueue orders")
                && traceparent(stored).contains(s.getSpanId()));
        assertThat(traceparent(stored)).isEqualTo("00-" + mineSpan.getTraceId() + "-" + mineSpan.getSpanId() + "-"
                + mineSpan.getSpanContext().getTraceFlags().asHex());
    }

    /** Records whose stored context the propagator cannot read have the step as parent, and no link to it. */
    @Test
    void noLinkForAMalformedTraceparent() {
        var delivered = deliveredInAStep(propagator,
                record(List.of(header("traceparent", "00-" + TRACE_ID + "-" + SPAN_ID + "-0"))),
                record(List.of(header("traceparent", "00-" + TRACE_ID.toUpperCase() + "-" + SPAN_ID + "-01"))),
                record(List.of(header("traceparent", ""))));

        assertThat(delivered.records()).allSatisfy(r -> delivered.assertChildOfTheStepWithoutLink(r));
    }

    @Test
    void noLinkForABaggageOrTracestateOnlyRecord() {
        var delivered = deliveredInAStep(withBaggage(),
                record(List.of(header("baggage", "tenant=acme"))),
                record(List.of(header("tracestate", "vendor=1"))));

        assertThat(delivered.records()).allSatisfy(r -> delivered.assertChildOfTheStepWithoutLink(r));
    }

    /** The getter reads the last header of a name: a repeated traceparent whose last value is null gives no parent. */
    @Test
    void noLinkWhenTheLastTraceparentIsNull() {
        var delivered = deliveredInAStep(propagator,
                record(List.of(header("traceparent", TRACEPARENT), new OutboxHeader("traceparent", null))));

        assertThat(delivered.records()).allSatisfy(r -> delivered.assertChildOfTheStepWithoutLink(r));
    }

    @Test
    void aLinkForAWellFormedTraceparent() {
        var delivered = deliveredInAStep(withBaggage(),
                record(List.of(header("traceparent", TRACEPARENT), header("baggage", "tenant=acme"))),
                record(List.of(header("traceparent", UNSAMPLED + "-extra"), header("traceparent", TRACEPARENT))));

        assertThat(delivered.records()).allSatisfy(r -> delivered.assertParentedByTheStoredContextAndLinked(r));
    }

    /** B3 extracts both formats whichever it injects: each counts when well formed, a bare sampling state does not. */
    @Test
    void b3HeadersLinkOnlyWhenWellFormed() {
        var b3 = new OtelPropagator(ContextPropagators.create(B3Propagator.injectingSingleHeader()), otel);
        var b3Trace = "463ac35c9f6413ad48485a3953bb6124";
        var single = record(List.of(header("b3", b3Trace + "-" + SPAN_ID + "-1")));
        var shortId = record(List.of(header("b3", b3Trace.substring(16) + "-" + SPAN_ID)));
        var multi = record(List.of(header("X-B3-TraceId", b3Trace), header("X-B3-SpanId", SPAN_ID),
                header("X-B3-Sampled", "1")));
        var samplingOnly = record(List.of(header("b3", "1")));
        var zeroSpan = record(List.of(header("b3", b3Trace + "-" + "0".repeat(16))));
        var tooManyParts = record(List.of(header("b3", b3Trace + "-" + SPAN_ID + "-1-" + SPAN_ID + "-x")));
        var delivered = deliveredInAStep(b3, single, shortId, multi, samplingOnly, zeroSpan, tooManyParts);

        for (var linked : List.of(single, shortId, multi)) {
            delivered.assertParentedByTheStoredContextAndLinked(linked);
        }
        for (var unlinked : List.of(samplingOnly, zeroSpan, tooManyParts)) {
            delivered.assertChildOfTheStepWithoutLink(unlinked);
        }
    }

    @Test
    void deliveryParentedByStoredContext() {
        var stored = record(List.of(header("traceparent", TRACEPARENT)));
        var delivery = telemetry().deliveryStarted(List.of(stored), NOW);
        delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        var span = finished("outbox deliver orders");
        assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
        assertThat(span.getTraceId()).isEqualTo(TRACE_ID);
        assertThat(span.getParentSpanId()).isEqualTo(SPAN_ID);
        assertThat(attribute(span, "messaging.destination.name")).isEqualTo("orders");
        assertThat(attribute(span, "outbox.outcome")).isEqualTo("delivered");
        assertThat(attribute(span, "outbox.message_id")).isEqualTo(stored.messageId().toString());
        assertThat(attribute(span, "outbox.attempt")).isEqualTo("1");
        assertThat(attribute(span, "outbox.delay_ms")).isEqualTo("1000");
    }

    @Test
    void deliveryCarriesTheShard() {
        var stopped = new CopyOnWriteArrayList<OutboxDeliveryContext>();
        observations.observationConfig().observationHandler(new ObservationHandler<OutboxDeliveryContext>() {
            @Override
            public void onStop(OutboxDeliveryContext context) {
                stopped.add(context);
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return context instanceof OutboxDeliveryContext;
            }
        });
        var known = record(List.of(), 3);
        var unknown = record(List.of(), -1);
        var delivery = telemetry().deliveryStarted(List.of(known, unknown), NOW);
        delivery.handed(List.of(known, unknown));
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));

        assertThat(stopped).hasSize(2);
        assertThat(stopped.get(0).getHighCardinalityKeyValues()).contains(KeyValue.of("outbox.shard", "3"));
        assertThat(stopped.get(1).getHighCardinalityKeyValue("outbox.shard")).isNull();
    }

    @Test
    void handedRecordCarriesDeliveryContext() {
        var stored = record(List.of(header("a", "1"), header("traceparent", TRACEPARENT)));
        var delivery = telemetry().deliveryStarted(List.of(stored), NOW);
        var handed = delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        var span = finished("outbox deliver orders");
        assertThat(traceparent(handed.get(0).headers()))
                .isEqualTo("00-" + span.getTraceId() + "-" + span.getSpanId() + "-01");
        assertThat(handed.get(0).headers()).extracting(OutboxHeader::name).containsOnlyOnce("a");
        assertThat(traceparent(stored.headers())).as("the stored record keeps its own").isEqualTo(TRACEPARENT);
    }

    @Test
    void storedBaggageSurvivesHanding() {
        var withBaggage = new OtelPropagator(ContextPropagators.create(TextMapPropagator.composite(
                W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())), otel);
        var registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                new PropagatingReceiverTracingObservationHandler<>(tracer, withBaggage),
                new DefaultTracingObservationHandler(tracer)));
        var telemetry = ObservationOutboxTelemetry.builder(registry, new OutboxMetrics(meters))
                .tracing(tracer, withBaggage).build();
        var stored = record(List.of(header("traceparent", TRACEPARENT), header("baggage", "tenant=acme")));
        var delivery = telemetry.deliveryStarted(List.of(stored), NOW);
        var handed = delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        var span = finished("outbox deliver orders");
        assertThat(traceparent(handed.get(0).headers()))
                .isEqualTo("00-" + span.getTraceId() + "-" + span.getSpanId() + "-01");
        assertThat(handed.get(0).headers()).filteredOn(h -> h.name().equals("baggage"))
                .extracting(h -> new String(h.value(), UTF_8)).containsExactly("tenant=acme");
    }

    @Test
    void deliveryWithoutItsOwnSpanKeepsTheStoredTraceInsideAStep() {
        observations.observationConfig().observationPredicate((name, context) -> !name.equals("outbox.deliver"));
        var telemetry = telemetry();
        var stored = record(List.of(header("traceparent", TRACEPARENT)));
        var step = telemetry.stepStarted(List.of(1));
        List<OutboxRecord> handed;
        try {
            assertThat(tracer.currentSpan()).as("the step span is current").isNotNull();
            var delivery = telemetry.deliveryStarted(List.of(stored), NOW);
            handed = delivery.handed(List.of(stored));
            delivery.finished(List.of(Outcome.delivered()));
        } finally {
            step.finished();
        }

        assertThat(traceparent(handed.get(0).headers())).isEqualTo(TRACEPARENT);
    }

    @Test
    void anUnsampledRecordIsObservedWhenAsked() {
        var stored = record(List.of(header("traceparent", UNSAMPLED)));
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator)
                .observeUnsampled(true)
                .build();
        var delivery = telemetry.deliveryStarted(List.of(stored), NOW);
        delivery.handed(List.of(stored));
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(meters.get("outbox.deliver").timer().count()).isEqualTo(1);
        assertThat(finished("outbox deliver orders").getParentSpanId()).isEqualTo(SPAN_ID);
    }

    @Test
    void anUnsampledRecordIsNotObservedByDefaultAndKeepsItsHeaders() throws Exception {
        var headers = List.of(header("a", "1"), header("traceparent", UNSAMPLED), header("baggage", "tenant=acme"));
        var stored = record(headers);
        var delivery = telemetry().deliveryStarted(List.of(stored), NOW);
        var handed = delivery.handed(List.of(stored));
        var inside = delivery.inRecordScope(handed.get(0), () -> tracer.currentSpan());
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(handed.get(0)).isSameAs(stored);
        assertThat(handed.get(0).headers()).isSameAs(headers);
        assertThat(inside).as("no scope of its own").isNull();
        assertThat(meters.find("outbox.deliver").timer()).isNull();
        assertThat(spans.getFinishedSpanItems()).isEmpty();
    }

    @Test
    void sampledMissingAndMalformedTraceparentsAreObserved() {
        var records = List.of(
                record(List.of(header("traceparent", TRACEPARENT))),
                record(List.of(header("b3", TRACE_ID + "-" + SPAN_ID + "-0"))),
                record(List.of(header("traceparent", "00-" + TRACE_ID + "-" + SPAN_ID + "-0"))),
                record(List.of(header("traceparent", "00-" + TRACE_ID.toUpperCase() + "-" + SPAN_ID + "-00"))),
                record(List.of(header("traceparent", "00-" + "0".repeat(32) + "-" + SPAN_ID + "-00"))),
                record(List.of(header("traceparent", "00-" + TRACE_ID + "-" + "0".repeat(16) + "-00"))),
                record(List.of(header("traceparent", "ff-" + TRACE_ID + "-" + SPAN_ID + "-00"))),
                record(List.of(header("traceparent", UNSAMPLED + "-extra"))),
                record(List.of(header("traceparent", UNSAMPLED), header("traceparent", TRACEPARENT))));
        var delivery = telemetry().deliveryStarted(records, NOW);
        delivery.handed(records);
        delivery.finished(records.stream().map(r -> Outcome.delivered()).toList());

        assertThat(meters.get("outbox.deliver").timer().count()).isEqualTo(records.size());
    }

    @Test
    void aMixedCallKeepsOrderAndTheOutcomesOfObservedRecords() {
        var a = record(List.of(header("traceparent", UNSAMPLED)));
        var b = record(List.of(header("traceparent", TRACEPARENT)));
        var c = record(List.of(header("traceparent", UNSAMPLED)));
        var d = record(List.of());
        var delivery = telemetry().deliveryStarted(List.of(a, b, c, d), NOW);
        var handed = delivery.handed(List.of(a, b, c, d));
        delivery.finished(List.of(Outcome.delivered(), Outcome.retry(new IOException("x")),
                Outcome.permanent(new IOException("y")), Outcome.requeue()));

        assertThat(handed).hasSize(4);
        assertThat(handed.get(0)).isSameAs(a);
        assertThat(handed.get(1).messageId()).isEqualTo(b.messageId());
        assertThat(handed.get(2)).isSameAs(c);
        assertThat(handed.get(3).messageId()).isEqualTo(d.messageId());
        assertThat(traceparent(handed.get(1).headers())).isNotEqualTo(TRACEPARENT);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "retry").timer().count()).isEqualTo(1);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "requeued").timer().count()).isEqualTo(1);
        assertThat(meters.find("outbox.deliver").tag("outbox.outcome", "delivered").timer()).isNull();
        assertThat(meters.find("outbox.deliver").tag("outbox.outcome", "permanent").timer()).isNull();
        assertThat(spans.getFinishedSpanItems()).extracting(s -> attribute(s, "outbox.message_id"))
                .containsExactlyInAnyOrder(b.messageId().toString(), d.messageId().toString());
    }

    @Test
    void withoutTracingHandedRecordsAreTheInput() {
        var plain = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters)).build();
        var records = List.of(record(List.of(header("traceparent", TRACEPARENT))));
        var delivery = plain.deliveryStarted(records, NOW);

        var handed = delivery.handed(records);
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(handed).isSameAs(records);
    }

    @Test
    void recordScopeMakesDeliveryCurrent() throws Exception {
        var records = List.of(record(List.of(header("traceparent", TRACEPARENT))));
        var delivery = telemetry().deliveryStarted(records, NOW);
        var handed = delivery.handed(records);

        var inside = delivery.inRecordScope(handed.get(0), () -> tracer.currentSpan().context().spanId());
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(inside).isEqualTo(finished("outbox deliver orders").getSpanId());
        assertThat(tracer.currentSpan()).as("the scope is closed after the call").isNull();
    }

    @Test
    void outcomeAndErrorRecorded() {
        var telemetry = telemetry();
        var retried = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        retried.handed(List.of(record(List.of())));
        retried.finished(List.of(Outcome.retry(new IOException("x"))));

        var span = finished("outbox deliver orders");
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(span.getEvents()).anyMatch(e -> e.getName().equals("exception"));
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "retry").timer().count()).isEqualTo(1);

        var failed = telemetry.deliveryStarted(List.of(record(List.of()), record(List.of())), NOW);
        failed.failed(new IllegalStateException("closed"));
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "failed").timer().count()).isEqualTo(2);
    }

    @Test
    void aRejectedContractIsCounted() {
        telemetry().contractRejected("orders", "Created", 2);

        assertThat(meters.get("outbox.messages.contract_rejected").tags("topic", "orders", "type", "Created",
                "version", "2").counter().count()).isEqualTo(1);
    }

    @Test
    void aRejectedAndAnUncheckedRouteAreCounted() {
        telemetry().routeRejected("orders");
        telemetry().routeUnchecked("audit");

        assertThat(meters.get("outbox.messages.route_rejected").tag("topic", "orders").counter().count()).isEqualTo(1);
        assertThat(meters.get("outbox.messages.route_unchecked").tag("topic", "audit").counter().count()).isEqualTo(1);
    }

    @Test
    void aWaitingRecordIsLabelledWaitingWithoutAnErrorAndCounted() {
        var telemetry = telemetry();
        var delivery = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        delivery.handed(List.of(record(List.of())));
        delivery.finished(List.of(Outcome.waitFor("v3 is not readable here")));
        telemetry.waited("orders", Outcome.Waiting.Kind.CONTRACT);

        var span = finished("outbox deliver orders");
        assertThat(attribute(span, "outbox.outcome")).isEqualTo("waiting");
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "waiting").timer().count()).isEqualTo(1);
        assertThat(meters.get("outbox.messages.waiting").tags("topic", "orders", "kind", "contract").counter().count())
                .isEqualTo(1);
    }

    @Test
    void stepObservationIsCurrentUntilItEnds() {
        var stopped = stoppedSteps();
        var step = telemetry().stepStarted(List.of(1, 3));
        step.fetched(1, 2);
        step.fetched(3, 5);

        var current = observations.getCurrentObservation();
        assertThat(current).isNotNull();
        assertThat(current.getContext()).isInstanceOfSatisfying(OutboxStepContext.class,
                c -> assertThat(c.shards()).containsExactly(1, 3));
        step.finished();

        assertThat(observations.getCurrentObservation()).isNull();
        assertThat(stopped).hasSize(1);
        var context = stopped.get(0);
        assertThat(context.getName()).isEqualTo("outbox.relay.step");
        assertThat(context.getContextualName()).isEqualTo("outbox relay step");
        assertThat(context.getLowCardinalityKeyValues()).containsExactly(KeyValue.of("messaging.system", "outbox"));
        assertThat(context.getHighCardinalityKeyValues()).containsExactlyInAnyOrder(
                KeyValue.of("outbox.shards", "1,3"), KeyValue.of("outbox.records", "7"));
        assertThat(context.getError()).isNull();
        var span = finished("outbox relay step");
        assertThat(span.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(span.getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
    }

    @Test
    void aStepOfOneShardCarriesTheShard() {
        var step = telemetry().stepStarted(List.of(2));
        step.fetched(2, 4);
        step.finished();

        var span = finished("outbox relay step");
        assertThat(attribute(span, "outbox.shard")).isEqualTo("2");
        assertThat(attribute(span, "outbox.records")).isEqualTo("4");
    }

    /** The settle is current from its start to its end, a client span under the step, which ends after it. */
    @Test
    void settleIsAChildOfTheStepAndCurrentUntilItEnds() {
        var telemetry = telemetry();
        var step = telemetry.stepStarted(List.of(1));
        var stepObservation = observations.getCurrentObservation();
        var settle = step.settleStarted();
        assertThat(observations.getCurrentObservation().getContext()).isInstanceOf(OutboxSettleContext.class);
        assertThat(observations.getCurrentObservation().getContextView().getParentObservation())
                .isSameAs(stepObservation);
        settle.committed(4);
        assertThat(observations.getCurrentObservation()).as("the step again").isSameAs(stepObservation);
        step.finished();
        assertThat(observations.getCurrentObservation()).isNull();

        var settleSpan = finished("outbox relay settle");
        var stepSpan = finished("outbox relay step");
        assertThat(settleSpan.getKind()).isEqualTo(SpanKind.CLIENT);
        assertThat(settleSpan.getParentSpanId()).isEqualTo(stepSpan.getSpanId());
        assertThat(attribute(settleSpan, "outbox.records")).isEqualTo("4");
        assertThat(attribute(settleSpan, "messaging.system")).isEqualTo("outbox");
        assertThat(settleSpan.getStatus().getStatusCode()).isNotEqualTo(StatusCode.ERROR);
        assertThat(stepSpan.getEndEpochNanos()).isGreaterThanOrEqualTo(settleSpan.getEndEpochNanos());
        assertThat(meters.get("outbox.relay.settle").timer().count()).isEqualTo(1);
    }

    @Test
    void failedSettleRecordsTheError() {
        var step = telemetry().stepStarted(List.of(1));
        var error = new SQLException("commit");
        step.settleStarted().failed(3, error);
        step.failed(error);

        var settle = finished("outbox relay settle");
        assertThat(settle.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(attribute(settle, "outbox.records")).isEqualTo("3");
        assertThat(observations.getCurrentObservation()).isNull();
        assertThat(tracer.currentSpan()).isNull();
    }

    @Test
    void customSettleConventionRenames() {
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .settleConvention(new OutboxSettleObservationConvention() {
                    @Override
                    public String getName() {
                        return "custom.settle";
                    }
                })
                .build();
        var step = telemetry.stepStarted(List.of(1));
        step.settleStarted().committed(1);
        step.finished();

        assertThat(meters.get("custom.settle").timer().count()).isEqualTo(1);
        assertThat(spans.getFinishedSpanItems()).extracting(SpanData::getName).contains("custom.settle")
                .doesNotContain("outbox relay settle");
    }

    /** The first report ends the record; the second and the call's own outcome change nothing. */
    @Test
    void recordFinishedTwiceEndsOnce() {
        var records = List.of(record(List.of()));
        var delivery = telemetry().deliveryStarted(records, NOW);
        var handed = delivery.handed(records);

        delivery.recordFinished(handed.get(0), Outcome.delivered());
        assertThat(spans.getFinishedSpanItems()).as("ended at the report").hasSize(1);
        delivery.recordFinished(handed.get(0), Outcome.retry(new IOException("again")));
        delivery.finished(List.of(Outcome.retry(new IOException("timed out"))));

        assertThat(spans.getFinishedSpanItems()).singleElement()
                .satisfies(s -> assertThat(attribute(s, "outbox.outcome")).isEqualTo("delivered"));
        assertThat(meters.get("outbox.deliver").timers()).singleElement()
                .satisfies(t -> assertThat(t.getId().getTag("outbox.outcome")).isEqualTo("delivered"))
                .satisfies(t -> assertThat(t.count()).isEqualTo(1));
    }

    @Test
    void lateRecordFinishedAfterFinishedIsIgnored() {
        var records = List.of(record(List.of()), record(List.of(header("traceparent", UNSAMPLED))));
        var delivery = telemetry().deliveryStarted(records, NOW);
        var handed = delivery.handed(records);
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));

        delivery.recordFinished(handed.get(0), Outcome.permanent(new IOException("late")));
        delivery.recordFinished(handed.get(1), Outcome.delivered()); // not observed
        delivery.recordFinished(record(List.of()), Outcome.delivered()); // not of this call
        var failed = telemetry().deliveryStarted(List.of(record(List.of())), NOW);
        failed.failed(new IllegalStateException("closed"));
        failed.recordFinished(record(List.of()), Outcome.delivered());

        assertThat(spans.getFinishedSpanItems()).hasSize(2);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "delivered").timer().count()).isEqualTo(1);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "failed").timer().count()).isEqualTo(1);
        assertThat(meters.find("outbox.deliver").tag("outbox.outcome", "permanent").timer()).isNull();
    }

    /** Each record's report ends it there; the call's end ends the records not reported. */
    @Test
    void recordFinishedEndsItsRecordAtOnce() {
        var records = List.of(record(List.of()), record(List.of()));
        var delivery = telemetry().deliveryStarted(records, NOW);
        var handed = delivery.handed(records);

        delivery.recordFinished(handed.get(1), Outcome.retry(new IOException("x")));
        assertThat(spans.getFinishedSpanItems()).singleElement()
                .satisfies(s -> assertThat(attribute(s, "outbox.message_id")).isEqualTo(
                        records.get(1).messageId().toString()))
                .satisfies(s -> assertThat(s.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR));
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));

        assertThat(spans.getFinishedSpanItems()).hasSize(2);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "retry").timer().count()).isEqualTo(1);
        assertThat(meters.get("outbox.deliver").tag("outbox.outcome", "delivered").timer().count()).isEqualTo(1);
    }

    /** Reports on a handler thread racing the call's end on the relay thread: each record stops exactly once. */
    @Test
    void recordFinishedRacingTheCallsEndStopsEachRecordOnce() throws Exception {
        var stops = new ConcurrentHashMap<OutboxDeliveryContext, AtomicInteger>();
        observations.observationConfig().observationHandler(new ObservationHandler<OutboxDeliveryContext>() {
            @Override
            public void onStop(OutboxDeliveryContext context) {
                stops.computeIfAbsent(context, c -> new AtomicInteger()).incrementAndGet();
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return context instanceof OutboxDeliveryContext;
            }
        });
        var telemetry = telemetry();
        int n = 200;
        for (int round = 0; round < 20; round++) {
            var records = IntStream.range(0, n).mapToObj(i -> record(List.of())).toList();
            var delivery = telemetry.deliveryStarted(records, NOW);
            var handed = delivery.handed(records);
            var go = new CountDownLatch(1);
            var reporter = new Thread(() -> {
                try {
                    go.await();
                } catch (InterruptedException e) {
                    return;
                }
                for (var r : handed) {
                    delivery.recordFinished(r, Outcome.delivered());
                }
            });
            reporter.start();
            go.countDown();
            delivery.finished(records.stream().map(r -> (Outcome) Outcome.retry(new IOException("x"))).toList());
            reporter.join();
        }

        assertThat(stops).hasSize(20 * n);
        assertThat(stops.values()).allSatisfy(c -> assertThat(c).hasValue(1));
        assertThat(meters.get("outbox.deliver").timers().stream().mapToLong(t -> t.count()).sum()).isEqualTo(20L * n);
    }

    /**
     * Inside a step, a delivery parented by its stored context links to the step's span; one without stored context
     * has the step as parent and no link; one started without a step has no link.
     */
    @Test
    void deliveryLinksToItsStep() {
        var telemetry = telemetry();
        var stored = record(List.of(header("traceparent", TRACEPARENT)));
        var bare = record(List.of());
        var step = telemetry.stepStarted(List.of(1));
        var delivery = telemetry.deliveryStarted(step, List.of(stored, bare), NOW);
        delivery.handed(List.of(stored, bare));
        delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));
        step.finished();
        var aloneRecord = record(List.of(header("traceparent", TRACEPARENT)));
        var alone = telemetry.deliveryStarted(List.of(aloneRecord), NOW);
        alone.finished(List.of(Outcome.delivered()));

        var stepSpan = finished("outbox relay step");
        var linked = span(s -> stored.messageId().toString().equals(attribute(s, "outbox.message_id")));
        assertThat(linked.getParentSpanId()).isEqualTo(SPAN_ID);
        assertThat(linked.getLinks()).singleElement()
                .satisfies(l -> assertThat(l.getSpanContext().getSpanId()).isEqualTo(stepSpan.getSpanId()));
        var child = span(s -> bare.messageId().toString().equals(attribute(s, "outbox.message_id")));
        assertThat(child.getParentSpanId()).isEqualTo(stepSpan.getSpanId());
        assertThat(child.getLinks()).isEmpty();
        assertThat(attribute(child, "messaging.system")).isEqualTo("outbox");
        var unlinked = span(s -> aloneRecord.messageId().toString().equals(attribute(s, "outbox.message_id")));
        assertThat(unlinked.getParentSpanId()).isEqualTo(SPAN_ID);
        assertThat(unlinked.getLinks()).isEmpty();
    }

    @Test
    void failedStepRecordsTheErrorAndLeavesNoScope() {
        var stopped = stoppedSteps();
        var error = new SQLException("x");

        telemetry().stepStarted(List.of(2)).failed(error);

        assertThat(observations.getCurrentObservation()).isNull();
        assertThat(tracer.currentSpan()).isNull();
        assertThat(stopped).hasSize(1);
        assertThat(stopped.get(0).getError()).isSameAs(error);
        assertThat(finished("outbox relay step").getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    }

    @Test
    void relayFailuresReachTheCounter() {
        telemetry().relayFailed(new SQLException("x"));

        assertThat(meters.counter("outbox.relay.failures").count()).isEqualTo(1);
    }

    @Test
    void customConventionRenames() {
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator)
                .deliveryConvention(new OutboxDeliveryObservationConvention() {
                    @Override
                    public String getName() {
                        return "custom.process";
                    }
                })
                .build();
        var delivery = telemetry.deliveryStarted(List.of(record(List.of())), NOW);
        delivery.finished(List.of(Outcome.delivered()));

        assertThat(meters.get("custom.process").timer().count()).isEqualTo(1);
        assertThat(spans.getFinishedSpanItems()).extracting(SpanData::getName).containsExactly("custom.process");
    }

    @Test
    void aConventionFailingOnOneRecordAtStartLeavesNoObservationRunning() {
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .deliveryConvention(new OutboxDeliveryObservationConvention() {
                    @Override
                    public String getName() {
                        return "outbox.deliver";
                    }

                    @Override
                    public KeyValues getLowCardinalityKeyValues(OutboxDeliveryContext context) {
                        if (context.record().topic().equals("bad")) {
                            throw new IllegalStateException("unknown topic");
                        }
                        return KeyValues.empty();
                    }
                })
                .build();
        var bad = new OutboxRecord(2, UUID.randomUUID(), "bad", null, null, List.of(), NOW.minusSeconds(1), 0, 0);

        assertThatThrownBy(() -> telemetry.deliveryStarted(List.of(record(List.of()), bad), NOW))
                .isInstanceOf(IllegalStateException.class);

        assertThat(meters.get("outbox.deliver.active").longTaskTimer().activeTasks()).isZero();
    }

    @Test
    void aHandlerFailingOnStartLeavesNoObservationRunning() {
        // registered after the meter handler, whose onStart has already started the active sample when this throws
        observations.observationConfig().observationHandler(new ObservationHandler<>() {
            @Override
            public void onStart(Observation.Context context) {
                if (!(context instanceof OutboxDeliveryContext d) || d.record().topic().equals("bad")) {
                    throw new IllegalStateException("onStart");
                }
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return true;
            }
        });
        var telemetry = telemetry();
        var bad = new OutboxRecord(2, UUID.randomUUID(), "bad", null, null, List.of(), NOW.minusSeconds(1), 0, 0);

        var enqueue = telemetry.enqueueStarted();
        assertThatThrownBy(() -> enqueue.message(OutboxMessage.of("orders", "k", bytes("p")), UUID.randomUUID(), 0))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> telemetry.stepStarted(List.of(1))).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> telemetry.deliveryStarted(List.of(record(List.of()), bad), NOW))
                .isInstanceOf(IllegalStateException.class);

        assertThat(meters.get("outbox.enqueue.active").longTaskTimer().activeTasks()).isZero();
        assertThat(meters.get("outbox.relay.step.active").longTaskTimer().activeTasks()).isZero();
        assertThat(meters.get("outbox.deliver.active").longTaskTimer().activeTasks()).isZero();
        assertThat(observations.getCurrentObservation()).isNull();
    }

    @Test
    void aConventionFailingOnOneRecordAtStopStillStopsTheOthers() {
        var telemetry = ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .deliveryConvention(new OutboxDeliveryObservationConvention() {
                    @Override
                    public String getName() {
                        return "outbox.deliver";
                    }

                    @Override
                    public String getContextualName(OutboxDeliveryContext context) {
                        if (context.outcome() instanceof Outcome.Retry) {
                            throw new IllegalStateException("no name for a retry");
                        }
                        return "deliver";
                    }
                })
                .build();
        var delivery = telemetry.deliveryStarted(List.of(record(List.of()), record(List.of())), NOW);
        delivery.handed(List.of(record(List.of()), record(List.of())));

        assertThatThrownBy(() -> delivery.finished(
                List.of(Outcome.retry(new IOException("x")), Outcome.delivered())))
                .isInstanceOf(IllegalStateException.class);

        // the retried record's own stop failed inside Micrometer; the delivered one is stopped all the same
        assertThat(meters.get("outbox.deliver.active").longTaskTimer().activeTasks()).isEqualTo(1);
        assertThat(meters.get("outbox.deliver").timer().count()).isEqualTo(1);
    }

    @Test
    void aHandlerFailingOnErrorStillStopsEveryObservation() {
        observations.observationConfig().observationHandler(new ObservationHandler<>() {
            @Override
            public void onError(Observation.Context context) {
                throw new IllegalStateException("handler");
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return true;
            }
        });
        var telemetry = telemetry();
        var error = new SQLException("x");

        var message = telemetry.enqueueStarted()
                .message(OutboxMessage.of("orders", "k", bytes("p")), UUID.randomUUID(), 0);
        assertThatThrownBy(() -> message.failed(error)).isInstanceOf(IllegalStateException.class);
        var step = telemetry.stepStarted(List.of(1));
        assertThatThrownBy(() -> step.failed(error)).isInstanceOf(IllegalStateException.class);
        var delivery = telemetry.deliveryStarted(List.of(record(List.of()), record(List.of())), NOW);
        assertThatThrownBy(() -> delivery.failed(error)).isInstanceOf(IllegalStateException.class);

        assertThat(meters.get("outbox.enqueue.active").longTaskTimer().activeTasks()).isZero();
        assertThat(meters.get("outbox.relay.step.active").longTaskTimer().activeTasks()).isZero();
        assertThat(meters.get("outbox.deliver.active").longTaskTimer().activeTasks()).isZero();
        assertThat(observations.getCurrentObservation()).isNull();
    }

    @Test
    void aHandlerFailingToOpenOrCloseTheStepScopeLeavesNoStepCurrentOrRunning() {
        for (boolean onOpen : new boolean[] {true, false}) {
            var registry = traced(ObservationRegistry.create()); // the tracer's thread-local must be left clean too
            throwingOnScope(registry, OutboxStepContext.class, onOpen);
            var stopped = stoppedSteps(registry);
            var telemetry = GuardedTelemetry.of(ObservationOutboxTelemetry.builder(registry, new OutboxMetrics(meters))
                    .tracing(tracer, propagator).build());
            spans.reset();

            telemetry.stepStarted(List.of(1)).finished();
            telemetry.stepStarted(List.of(2)).failed(new SQLException("x"));

            assertThat(registry.getCurrentObservation()).as("onOpen=" + onOpen).isNull();
            assertThat(tracer.currentSpan()).as("onOpen=" + onOpen).isNull();
            assertThat(stopped).as("onOpen=" + onOpen).hasSize(2);
            assertThat(spans.getFinishedSpanItems()).as("onOpen=" + onOpen).hasSize(2)
                    .allSatisfy(span -> assertThat(span.getParentSpanContext().isValid()).isFalse());
        }
    }

    @Test
    void aHandlerFailingToOpenOrCloseTheRecordScopeLeavesNoDeliveryCurrent() throws Exception {
        for (boolean onOpen : new boolean[] {true, false}) {
            var registry = traced(ObservationRegistry.create());
            throwingOnScope(registry, OutboxDeliveryContext.class, onOpen);
            var telemetry = GuardedTelemetry.of(ObservationOutboxTelemetry.builder(registry, new OutboxMetrics(meters))
                    .tracing(tracer, propagator).build());
            var records = List.of(record(List.of()), record(List.of()));
            var delivery = telemetry.deliveryStarted(records, NOW);
            var handed = delivery.handed(records);

            for (var record : handed) {
                assertThat(delivery.inRecordScope(record, () -> 1)).isEqualTo(1);
                assertThat(registry.getCurrentObservation()).as("onOpen=" + onOpen).isNull();
                assertThat(tracer.currentSpan()).as("onOpen=" + onOpen).isNull();
            }
            delivery.finished(List.of(Outcome.delivered(), Outcome.delivered()));
        }
    }

    @Test
    void releasingAHandlerThreadDropsTheObservationItsCallLeftCurrent() {
        var registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(context -> true); // a real observation, not a no-op one
        Observation.start("left.open", registry).openScope(); // a handler that never closes its scope

        ObservationOutboxTelemetry.builder(registry, new OutboxMetrics(meters)).build().releaseThread();

        assertThat(registry.getCurrentObservation()).isNull();
    }

    @Test
    void meterFilterRenamesGauge() {
        meters.config().meterFilter(new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                return id.getName().equals("outbox.shard.lag") ? id.withName("custom.lag") : id;
            }
        });

        telemetry().gauge(OutboxGauge.SHARD_LAG, () -> 7);

        assertThat(meters.get("custom.lag").gauge().value()).isEqualTo(7);
    }

    private ObservationOutboxTelemetry telemetry() {
        return ObservationOutboxTelemetry.builder(observations, new OutboxMetrics(meters))
                .tracing(tracer, propagator)
                .build();
    }

    /** W3C trace context and baggage, as Spring Boot's default propagation. */
    private Propagator withBaggage() {
        return new OtelPropagator(ContextPropagators.create(TextMapPropagator.composite(
                W3CTraceContextPropagator.getInstance(), W3CBaggagePropagator.getInstance())), otel);
    }

    /** {@code records} delivered in one call of a step, traced with {@code propagator} and the delivery handler. */
    private Delivered deliveredInAStep(Propagator propagator, OutboxRecord... records) {
        var registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new ObservationHandler.FirstMatchingCompositeObservationHandler(
                new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
                new OutboxDeliveryTracingObservationHandler(tracer, propagator),
                new DefaultTracingObservationHandler(tracer)));
        var telemetry = ObservationOutboxTelemetry.builder(registry, new OutboxMetrics(meters))
                .observeUnsampled(true).build();
        var list = List.of(records);
        var step = telemetry.stepStarted(List.of(1));
        var delivery = telemetry.deliveryStarted(step, list, NOW);
        delivery.handed(list);
        delivery.finished(list.stream().map(r -> Outcome.delivered()).toList());
        step.finished();
        return new Delivered(list, finished("outbox relay step"));
    }

    private final class Delivered {

        private final List<OutboxRecord> records;
        private final SpanData step;

        Delivered(List<OutboxRecord> records, SpanData step) {
            this.records = records;
            this.step = step;
        }

        List<OutboxRecord> records() {
            return records;
        }

        void assertChildOfTheStepWithoutLink(OutboxRecord record) {
            var span = delivery(record);
            assertThat(span.getParentSpanId()).as(headersOf(record)).isEqualTo(step.getSpanId());
            assertThat(span.getLinks()).as(headersOf(record)).isEmpty();
        }

        void assertParentedByTheStoredContextAndLinked(OutboxRecord record) {
            var span = delivery(record);
            assertThat(span.getParentSpanId()).as(headersOf(record)).isEqualTo(SPAN_ID);
            assertThat(span.getLinks()).as(headersOf(record)).singleElement()
                    .satisfies(l -> assertThat(l.getSpanContext().getSpanId()).isEqualTo(step.getSpanId()));
        }

        private SpanData delivery(OutboxRecord record) {
            return span(s -> record.messageId().toString().equals(attribute(s, "outbox.message_id")));
        }

        private String headersOf(OutboxRecord record) {
            return record.headers().stream()
                    .map(h -> h.name() + "=" + (h.value() == null ? null : new String(h.value(), UTF_8))).toList()
                    .toString();
        }
    }

    /** The step contexts as they stop. */
    private List<OutboxStepContext> stoppedSteps() {
        return stoppedSteps(observations);
    }

    private static List<OutboxStepContext> stoppedSteps(ObservationRegistry registry) {
        var stopped = new CopyOnWriteArrayList<OutboxStepContext>();
        registry.observationConfig().observationHandler(new ObservationHandler<OutboxStepContext>() {
            @Override
            public void onStop(OutboxStepContext context) {
                stopped.add(context);
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return context instanceof OutboxStepContext;
            }
        });
        return stopped;
    }

    /** A handler of {@code type} contexts that throws when their scope opens, or when it closes. */
    private static void throwingOnScope(ObservationRegistry registry, Class<?> type, boolean onOpen) {
        registry.observationConfig().observationHandler(new ObservationHandler<>() {
            @Override
            public void onScopeOpened(Observation.Context context) {
                if (onOpen) {
                    throw new IllegalStateException("onScopeOpened");
                }
            }

            @Override
            public void onScopeClosed(Observation.Context context) {
                if (!onOpen) {
                    throw new IllegalStateException("onScopeClosed");
                }
            }

            @Override
            public boolean supportsContext(Observation.Context context) {
                return type.isInstance(context);
            }
        });
    }

    private SpanData finished(String name) {
        return spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals(name)).findFirst().orElseThrow();
    }

    /** The one finished span matching {@code filter}. */
    private SpanData span(Predicate<SpanData> filter) {
        var found = spans.getFinishedSpanItems().stream().filter(filter).toList();
        assertThat(found).hasSize(1);
        return found.get(0);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    private static String attribute(SpanData span, String key) {
        return span.getAttributes().get(AttributeKey.stringKey(key));
    }

    private static OutboxRecord record(List<OutboxHeader> headers) {
        return new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, headers, NOW.minusSeconds(1), 0, 0);
    }

    private static OutboxRecord record(List<OutboxHeader> headers, int shard) {
        return new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, headers, NOW.minusSeconds(1), 0, 0,
                shard);
    }

    private static OutboxHeader header(String name, String value) {
        return new OutboxHeader(name, value.getBytes(UTF_8));
    }

    private static String traceparent(List<OutboxHeader> headers) {
        return headers.stream().filter(h -> h.name().equals("traceparent")).map(h -> new String(h.value(), UTF_8))
                .reduce((first, last) -> last).orElseThrow();
    }
}
