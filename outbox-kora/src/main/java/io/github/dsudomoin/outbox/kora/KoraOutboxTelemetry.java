package io.github.dsudomoin.outbox.kora;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.internal.RecordScope;
import io.github.dsudomoin.outbox.micrometer.OutboxMetrics;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.DoubleSupplier;
import java.util.stream.Collectors;
import ru.tinkoff.kora.common.Context;

/**
 * The Kora app's telemetry: the {@code outbox.*} meters of {@link OutboxMetrics} and, with a {@link Tracer}, four
 * spans, each with {@code messaging.system=outbox}:
 * <ul>
 *   <li>{@code outbox enqueue <topic>}, PRODUCER, one per sent message, a child of the current Kora context's span;
 *   the message stores its trace headers unless it carries a {@code traceparent}, and its own INSERT runs in a Kora
 *   context carrying it ({@code sendAll} writes all in one statement, outside any of them).</li>
 *   <li>{@code outbox relay step}, CLIENT, one per relay step that finds work, current in the Kora context of the
 *   thread that steps until its transaction ended; that thread's previous Kora context is restored then.</li>
 *   <li>{@code outbox relay settle}, CLIENT, a child of the step, current from the step's last handler call until its
 *   transaction ended.</li>
 *   <li>{@code outbox deliver <topic>}, CONSUMER, one per delivered record, a child of its stored trace with a link to
 *   the step's span, or of the step's span when the record stores no valid {@code traceparent}; it ends when the
 *   handler reports the record's outcome, else with the call, whichever comes first. The handled record carries its
 *   trace headers.</li>
 * </ul>
 * Without a tracer, each sent message stores the trace of the current Kora context unless it carries one. Either way,
 * each handled record runs in a Kora context carrying the trace it was handed with, so Kora clients called from the
 * handler continue it.
 */
public final class KoraOutboxTelemetry implements OutboxTelemetry {

    /**
     * Without a tracer: the records as stored, each handled in a Kora context carrying its stored trace; it hears no
     * reports, so a sender's calls enter no Kora context of their own.
     */
    private static final Delivery SCOPE_ONLY = new RecordScope.ScopeOnly() {
        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            return inKoraContext(record, call);
        }
    };

    private static final AttributeKey<String> SYSTEM = AttributeKey.stringKey("messaging.system");
    private static final AttributeKey<String> DESTINATION = AttributeKey.stringKey("messaging.destination.name");
    private static final AttributeKey<String> MESSAGE_ID = AttributeKey.stringKey("outbox.message_id");
    private static final AttributeKey<Long> SHARD = AttributeKey.longKey("outbox.shard");
    private static final AttributeKey<String> SHARDS = AttributeKey.stringKey("outbox.shards");
    private static final AttributeKey<Long> RECORDS = AttributeKey.longKey("outbox.records");
    private static final String OUTBOX = "outbox";

    private final OutboxMetrics metrics;
    @Nullable
    private final Tracer tracer;
    /** Stateless: each message has its own span. Null without a tracer. */
    @Nullable
    private final Enqueue spanEnqueue;

    public KoraOutboxTelemetry(OutboxMetrics metrics, @Nullable Tracer tracer) {
        this.metrics = metrics;
        this.tracer = tracer;
        this.spanEnqueue = tracer == null ? null : new SpanEnqueue(tracer);
    }

    /** With a tracer, a span per message; without, the current Kora context's trace, read once per call. */
    @Override
    public Enqueue enqueueStarted() {
        if (spanEnqueue != null) {
            return spanEnqueue;
        }
        var trace = OutboxTracing.currentHeaders();
        return trace.isEmpty() ? Enqueue.NONE : new TracedEnqueue(trace);
    }

    @Override
    public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
        return deliveryStarted(RelayStep.NONE, records, now);
    }

    @Override
    public Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, Instant now) {
        return tracer == null ? SCOPE_ONLY : new SpanDelivery(tracer, step instanceof SpanStep s ? s : null, records,
                now);
    }

    /** With a tracer, the step's span, made current in a forked Kora context; the caller's context is kept. */
    @Override
    public RelayStep stepStarted(List<Integer> shards) {
        if (tracer == null) {
            return RelayStep.NONE;
        }
        var parent = OutboxTracing.current();
        var builder = tracer.spanBuilder("outbox relay step")
                .setSpanKind(SpanKind.CLIENT)
                .setParent(parent)
                .setAttribute(SYSTEM, OUTBOX)
                .setAttribute(SHARDS, shards.stream().map(String::valueOf).collect(Collectors.joining(",")));
        if (shards.size() == 1) {
            builder.setAttribute(SHARD, (long) shards.get(0));
        }
        var span = builder.startSpan();
        var context = parent.with(span);
        OutboxTracing.KoraScope scope;
        try {
            scope = OutboxTracing.open(context);
        } catch (Throwable e) {
            span.end();
            throw e;
        }
        return new SpanStep(tracer, span, context, scope);
    }

    /** Nothing a call set in the Kora context leaks into the next call on the thread. */
    @Override
    public void releaseThread() {
        Context.clear();
    }

    @Override
    public void enqueued(String topic) {
        metrics.enqueued(topic);
    }

    @Override
    public void delivered(String topic, Duration delay) {
        metrics.delivered(topic, delay);
    }

    @Override
    public void retried(String topic) {
        metrics.retried(topic);
    }

    @Override
    public void deadLettered(String topic) {
        metrics.deadLettered(topic);
    }

    @Override
    public void requeued(String topic) {
        metrics.requeued(topic);
    }

    @Override
    public void waited(String topic, Outcome.Waiting.Kind kind) {
        metrics.waited(topic, kind);
    }

    @Override
    public void contractRejected(String topic, String type, int version) {
        metrics.contractRejected(topic, type, version);
    }

    @Override
    public void routeRejected(String topic) {
        metrics.routeRejected(topic);
    }

    @Override
    public void routeUnchecked(String topic) {
        metrics.routeUnchecked(topic);
    }

    @Override
    public void stepFinished(Duration duration) {
        metrics.stepFinished(duration);
    }

    @Override
    public void relayFailed(Throwable error) {
        metrics.relayFailed(error);
    }

    @Override
    public void shardPaused() {
        metrics.shardPaused();
    }

    @Override
    public void deadLetterDecision(String decision) {
        metrics.deadLetterDecision(decision);
    }

    @Override
    public void deadLetterHookFailed() {
        metrics.deadLetterHookFailed();
    }

    @Override
    public void inboxOutsideWindow() {
        metrics.inboxOutsideWindow();
    }

    @Override
    public void gauge(OutboxGauge gauge, DoubleSupplier value) {
        metrics.gauge(gauge, value);
    }

    static <T> T inKoraContext(OutboxRecord record, Callable<T> call) throws Exception {
        return OutboxTracing.withKoraContext(OutboxTracing.extract(record.headers()), call);
    }

    /** Adds the sender's trace to a message without a {@code traceparent}. */
    private record TracedEnqueue(List<OutboxHeader> trace) implements Enqueue {
        @Override
        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
            return withTrace(headers, trace);
        }
    }

    /** {@code headers} as they are when they have a {@code traceparent}, else followed by {@code trace}. */
    private static List<OutboxHeader> withTrace(List<OutboxHeader> headers, List<OutboxHeader> trace) {
        if (trace.isEmpty()) {
            return headers;
        }
        for (var h : headers) {
            if (h.name().equals(OutboxTracing.TRACEPARENT)) {
                return headers;
            }
        }
        var all = new ArrayList<OutboxHeader>(headers.size() + trace.size());
        all.addAll(headers);
        all.addAll(trace);
        return all;
    }

    /** A span per message, a child of the current Kora context's span; the call itself starts nothing. */
    private record SpanEnqueue(Tracer tracer) implements Enqueue {
        @Override
        public Message message(OutboxMessage message, UUID recordId, int shard) {
            var parent = OutboxTracing.current();
            var span = tracer.spanBuilder("outbox enqueue " + message.topic())
                    .setSpanKind(SpanKind.PRODUCER)
                    .setParent(parent)
                    .setAttribute(SYSTEM, OUTBOX)
                    .setAttribute(DESTINATION, message.topic())
                    .setAttribute(MESSAGE_ID, recordId.toString())
                    .setAttribute(SHARD, (long) shard)
                    .startSpan();
            return new SpanMessage(span, parent.with(span));
        }
    }

    /** One message's span: stored unless the message has a {@code traceparent}, current while its insert runs. */
    private record SpanMessage(Span span, io.opentelemetry.context.Context context) implements Message {

        @Override
        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
            return withTrace(headers, OutboxTracing.headers(context));
        }

        @Override
        public <T> T inScope(Callable<T> insert) throws Exception {
            return OutboxTracing.withKoraContext(context, insert);
        }

        @Override
        public void inserted() {
            span.end();
        }

        @Override
        public void failed(Throwable error) {
            fail(span, error);
            span.end();
        }
    }

    /**
     * The step's span, current on the thread that steps until the step ends there: the first end closes the open
     * settle, restores the thread's Kora context from before the step and ends the span; a later end does nothing.
     */
    private static final class SpanStep implements RelayStep {

        private final Tracer tracer;
        private final Span span;
        private final io.opentelemetry.context.Context context;
        private final OutboxTracing.KoraScope scope;
        /** The fields below: on the thread that steps. */
        private long records;
        @Nullable
        private SpanSettle settle;
        private boolean ended;

        SpanStep(Tracer tracer, Span span, io.opentelemetry.context.Context context, OutboxTracing.KoraScope scope) {
            this.tracer = tracer;
            this.span = span;
            this.context = context;
            this.scope = scope;
        }

        @Override
        public void fetched(int shard, int records) {
            this.records += records;
        }

        /** A child of the step, current in a Kora context forked from the step's until it ends. */
        @Override
        public Settle settleStarted() {
            var started = tracer.spanBuilder("outbox relay settle")
                    .setSpanKind(SpanKind.CLIENT)
                    .setParent(context)
                    .setAttribute(SYSTEM, OUTBOX)
                    .startSpan();
            OutboxTracing.KoraScope settleScope;
            try {
                settleScope = OutboxTracing.open(context.with(started));
            } catch (Throwable e) {
                started.end();
                throw e;
            }
            settle = new SpanSettle(started, settleScope);
            return settle;
        }

        @Override
        public void finished() {
            end(null);
        }

        @Override
        public void failed(Throwable error) {
            end(error);
        }

        private void end(@Nullable Throwable error) {
            if (ended) {
                return;
            }
            ended = true;
            try {
                if (settle != null) {
                    settle.end(-1, error);
                }
            } finally {
                scope.close();
                span.setAttribute(RECORDS, records);
                if (error != null) {
                    fail(span, error);
                }
                span.end();
            }
        }
    }

    /** The settle's span; the first end restores the step's Kora context, a later end does nothing. */
    private static final class SpanSettle implements Settle {

        private final Span span;
        private final OutboxTracing.KoraScope scope;
        private boolean ended;

        SpanSettle(Span span, OutboxTracing.KoraScope scope) {
            this.span = span;
            this.scope = scope;
        }

        @Override
        public void committed(int records) {
            end(records, null);
        }

        @Override
        public void failed(int records, Throwable error) {
            end(records, error);
        }

        /** {@code records} below zero: unknown, the step ended before its settle. */
        void end(int records, @Nullable Throwable error) {
            if (ended) {
                return;
            }
            ended = true;
            scope.close();
            if (records >= 0) {
                span.setAttribute(RECORDS, (long) records);
            }
            if (error != null) {
                fail(span, error);
            }
            span.end();
        }
    }

    private static void fail(Span span, Throwable error) {
        span.recordException(error);
        span.setStatus(StatusCode.ERROR);
    }


    /**
     * A delivery span per record, started before the call and ended once: by the handler's report of the record
     * ({@link #recordFinished}, on the handler thread) or by the call's end (on the relay thread), whichever comes
     * first.
     */
    private static final class SpanDelivery implements Delivery {

        private final Span[] spans;
        /** 1 where the span has ended. */
        private final AtomicIntegerArray ended;
        /**
         * The index of each record the handler may report: the stored ones and the handed ones. Written on the relay
         * thread before the records reach the handler thread, only read after.
         */
        private final Map<OutboxRecord, Integer> byRecord;

        SpanDelivery(Tracer tracer, @Nullable SpanStep step, List<OutboxRecord> records, Instant now) {
            spans = new Span[records.size()];
            ended = new AtomicIntegerArray(spans.length);
            byRecord = new IdentityHashMap<>(2 * spans.length);
            try {
                for (int i = 0; i < spans.length; i++) {
                    var r = records.get(i);
                    spans[i] = start(tracer, step, r, now);
                    byRecord.put(r, i);
                }
            } catch (Throwable e) {
                // the caller gets no Delivery to end them with
                for (var span : spans) {
                    if (span != null) {
                        span.end();
                    }
                }
                throw e;
            }
        }

        /** A child of the stored trace linked to the step's span, else of the step's span, else a root. */
        private static Span start(Tracer tracer, @Nullable SpanStep step, OutboxRecord r, Instant now) {
            var builder = tracer.spanBuilder("outbox deliver " + r.topic())
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute(SYSTEM, OUTBOX)
                    .setAttribute(DESTINATION, r.topic())
                    .setAttribute(MESSAGE_ID, r.messageId().toString())
                    .setAttribute("outbox.attempt", r.attempts() + 1L)
                    .setAttribute("outbox.delay_ms", delay(r, now).toMillis());
            if (r.shard() >= 0) {
                builder.setAttribute(SHARD, (long) r.shard());
            }
            var stored = OutboxTracing.extract(r.headers());
            if (OutboxTracing.hasValidSpan(stored)) {
                builder.setParent(stored);
                if (step != null) {
                    builder.addLink(step.span.getSpanContext());
                }
            } else if (step != null) {
                builder.setParent(step.context);
            } else {
                builder.setNoParent();
            }
            return builder.startSpan();
        }

        /** Each record with its span's trace headers in place of the stored ones. */
        @Override
        public List<OutboxRecord> handed(List<OutboxRecord> records) {
            var handed = new ArrayList<OutboxRecord>(records.size());
            for (int i = 0; i < records.size(); i++) {
                var r = records.get(i);
                var ctx = io.opentelemetry.context.Context.root().with(spans[i]);
                var h = r.withHeaders(OutboxTracing.replaceTraceHeaders(r.headers(), ctx));
                byRecord.put(h, i);
                handed.add(h);
            }
            return handed;
        }

        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            return inKoraContext(record, call);
        }

        /** Ends the record's span unless it has ended; a record not of this call is ignored. */
        @Override
        public void recordFinished(OutboxRecord record, Outcome outcome) {
            var i = byRecord.get(record);
            if (i != null && ended.compareAndSet(i, 0, 1)) {
                end(spans[i], outcome);
            }
        }

        @Override
        public void finished(List<Outcome> outcomes) {
            for (int i = 0; i < spans.length; i++) {
                if (ended.compareAndSet(i, 0, 1)) {
                    end(spans[i], outcomes.get(i));
                }
            }
        }

        @Override
        public void failed(Throwable error) {
            for (int i = 0; i < spans.length; i++) {
                if (ended.compareAndSet(i, 0, 1)) {
                    fail(spans[i], error);
                    spans[i].end();
                }
            }
        }

        private static void end(Span span, Outcome outcome) {
            Throwable error = null;
            String name;
            if (outcome instanceof Outcome.Retry r) {
                name = "retry";
                error = r.error();
            } else if (outcome instanceof Outcome.Permanent p) {
                name = "permanent";
                error = p.error();
            } else if (outcome instanceof Outcome.NotAttempted) {
                name = "not_attempted";
            } else if (outcome instanceof Outcome.Requeue) {
                name = "requeued";
            } else if (outcome instanceof Outcome.Waiting) {
                name = "waiting";
            } else {
                name = "delivered";
            }
            span.setAttribute("outbox.outcome", name);
            if (error != null) {
                span.recordException(error);
                span.setStatus(StatusCode.ERROR);
            }
            span.end();
        }

        /** {@code now - createdAt}, zero when the clocks disagree. */
        private static Duration delay(OutboxRecord r, Instant now) {
            var delay = Duration.between(r.createdAt(), now);
            return delay.isNegative() ? Duration.ZERO : delay;
        }
    }
}
