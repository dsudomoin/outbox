package io.github.dsudomoin.outbox.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;

/**
 * A telemetry model after the messaging semantic conventions, written as an outside team would write it: on the
 * public {@link OutboxTelemetry} SPI and the OpenTelemetry API only, with no Micrometer and no framework classes.
 *
 * <p>Spans: {@code enqueue <topic>} (PRODUCER, one per message, child of the current span, its context stored in the
 * row and current during the message's own INSERT); {@code fetch <table>} (CLIENT, one per relay step with work);
 * {@code deliver <topic>} (CONSUMER, one per record and attempt, ending at the record's own outcome, child of the
 * stored context and linked to the fetch span, else a child of the fetch span); {@code settle <table>} (CLIENT, child
 * of the fetch span). Meters, kept in memory ({@link Meters}): messages enqueued once committed (a failed write with
 * {@code error.type}), delivery attempts, dead letters by {@code error.type}, the fetch batch size, and each shard's
 * pending count and oldest age.
 */
final class MessagingConventionsTelemetry implements OutboxTelemetry {

    static final AttributeKey<String> SYSTEM = AttributeKey.stringKey("messaging.system");
    static final AttributeKey<String> OPERATION_NAME = AttributeKey.stringKey("messaging.operation.name");
    static final AttributeKey<String> OPERATION_TYPE = AttributeKey.stringKey("messaging.operation.type");
    static final AttributeKey<String> COLLECTION = AttributeKey.stringKey("db.collection.name");
    static final AttributeKey<String> DESTINATION = AttributeKey.stringKey("messaging.destination.name");
    static final AttributeKey<String> RECORD_ID = AttributeKey.stringKey("outbox.record.id");
    static final AttributeKey<String> PARTITION = AttributeKey.stringKey("outbox.partition.id");
    static final AttributeKey<Long> BATCH_COUNT = AttributeKey.longKey("messaging.batch.message_count");
    static final AttributeKey<Long> RESEND_COUNT = AttributeKey.longKey("messaging.request.resend_count");
    static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");

    static final String SYSTEM_NAME = "outbox";

    /** Counter: messages whose write committed; a failed write counts with {@code error.type}. */
    static final String ENQUEUED = "messaging.client.sent.messages";
    /** Counter: delivery attempts, by the outcome the call returned; a failed one with {@code error.type}. */
    static final String DELIVERED = "messaging.client.consumed.messages";
    /** Counter: records moved to the dead table, by {@code error.type}. */
    static final String DEAD_LETTERED = "messaging.client.dead_lettered.messages";
    /** Histogram: the records each step fetched. */
    static final String FETCH_BATCH_SIZE = "messaging.client.fetch.batch_size";

    private static final String TRACEPARENT = "traceparent";

    private static final TextMapGetter<List<OutboxHeader>> HEADERS = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(List<OutboxHeader> carrier) {
            return carrier.stream().map(OutboxHeader::name).distinct().toList();
        }

        @Nullable
        @Override
        public String get(@Nullable List<OutboxHeader> carrier, String key) {
            if (carrier == null) {
                return null;
            }
            for (int i = carrier.size() - 1; i >= 0; i--) { // the last header of a name, as a Kafka reader takes it
                var h = carrier.get(i);
                if (h.name().equals(key)) {
                    return h.value() == null ? null : new String(h.value(), UTF_8);
                }
            }
            return null;
        }
    };

    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final String table;
    private final Meters meters = new Meters();

    /** {@code table}: the outbox table, as configured; the SPI does not name it, the application knows it. */
    MessagingConventionsTelemetry(Tracer tracer, TextMapPropagator propagator, String table) {
        this.tracer = tracer;
        this.propagator = propagator;
        this.table = table;
    }

    Meters meters() {
        return meters;
    }

    // enqueue

    @Override
    public Enqueue enqueueStarted() {
        return new Enqueue() {
            @Override
            public Message message(OutboxMessage message, UUID recordId, int shard) {
                return new MessageSpan(message.topic(), recordId, shard);
            }
        };
    }

    @Override
    public void enqueued(String topic) {
        meters.add(ENQUEUED, Map.of(DESTINATION.getKey(), topic));
    }

    /** One message's {@code enqueue} span: started at the message, ended at its insert. */
    private final class MessageSpan implements Message {

        private final String topic;
        private final Span span;
        private final Context context;

        MessageSpan(String topic, UUID recordId, int shard) {
            this.topic = topic;
            var parent = Context.current();
            this.span = common(tracer.spanBuilder("enqueue " + topic), "enqueue", "send")
                    .setSpanKind(SpanKind.PRODUCER).setParent(parent)
                    .setAttribute(DESTINATION, topic)
                    .setAttribute(RECORD_ID, recordId.toString())
                    .setAttribute(PARTITION, Integer.toString(shard))
                    .startSpan();
            this.context = parent.with(span);
        }

        /** Stores the span's context in the row, unless the message brings trace context of its own. */
        @Override
        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
            if (HEADERS.get(headers, TRACEPARENT) != null) {
                return headers;
            }
            var stored = new ArrayList<>(headers);
            propagator.inject(context, stored, (carrier, key, value) -> {
                carrier.removeIf(h -> h.name().equals(key));
                carrier.add(new OutboxHeader(key, value.getBytes(UTF_8)));
            });
            return stored;
        }

        @Override
        public <T> T inScope(Callable<T> insert) throws Exception {
            try (var scope = context.makeCurrent()) {
                return insert.call();
            }
        }

        @Override
        public void inserted() {
            span.end();
        }

        @Override
        public void failed(Throwable error) {
            var type = error.getClass().getName();
            span.setAttribute(ERROR_TYPE, type).setStatus(StatusCode.ERROR).recordException(error);
            span.end();
            meters.add(ENQUEUED, Map.of(DESTINATION.getKey(), topic, ERROR_TYPE.getKey(), type));
        }
    }

    // relay step and settle

    @Override
    public RelayStep stepStarted(List<Integer> shards) {
        return new Step(shards);
    }

    /** A step's {@code fetch} span, on the relay thread from its start to its end. */
    private final class Step implements RelayStep {

        final Span span;
        private final List<Integer> shards;
        private long fetched;
        private boolean ended;

        Step(List<Integer> shards) {
            this.shards = List.copyOf(shards);
            this.span = common(tracer.spanBuilder("fetch " + table), "fetch", "receive")
                    .setSpanKind(SpanKind.CLIENT).setParent(Context.current()).startSpan();
        }

        @Override
        public void fetched(int shard, int records) {
            fetched += records;
        }

        @Override
        public Settle settleStarted() {
            return new SettleSpan(span);
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
            span.setAttribute(BATCH_COUNT, fetched);
            if (shards.size() == 1) {
                span.setAttribute(PARTITION, Integer.toString(shards.get(0)));
            }
            if (error != null) {
                span.setAttribute(ERROR_TYPE, error.getClass().getName()).setStatus(StatusCode.ERROR)
                        .recordException(error);
            }
            span.end();
            meters.record(FETCH_BATCH_SIZE, fetched);
        }
    }

    /** A step's {@code settle} span, a child of its {@code fetch} span. */
    private final class SettleSpan implements Settle {

        private final Span span;
        private boolean ended;

        SettleSpan(Span step) {
            this.span = common(tracer.spanBuilder("settle " + table), "settle", "settle")
                    .setSpanKind(SpanKind.CLIENT).setParent(Context.root().with(step)).startSpan();
        }

        @Override
        public void committed(int records) {
            end(records, null);
        }

        @Override
        public void failed(int records, Throwable error) {
            end(records, error);
        }

        private void end(int records, @Nullable Throwable error) {
            if (ended) {
                return;
            }
            ended = true;
            span.setAttribute(BATCH_COUNT, (long) records);
            if (error != null) {
                span.setAttribute(ERROR_TYPE, error.getClass().getName()).setStatus(StatusCode.ERROR)
                        .recordException(error);
            }
            span.end();
        }
    }

    // delivery

    @Override
    public Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, Instant now) {
        return new Call(step instanceof Step s ? s.span : null, records, now);
    }

    @Override
    public void deadLettered(OutboxRecord record, String errorType) {
        meters.add(DEAD_LETTERED, Map.of(DESTINATION.getKey(), record.topic(), ERROR_TYPE.getKey(), errorType));
    }

    /**
     * One handler call: a {@code deliver} span per record, keyed by the record object the relay hands (it returns the
     * records as they are, so the handed ones are these). The map is built before the call and only read after.
     */
    private final class Call implements Delivery {

        private final List<OutboxRecord> records;
        private final Map<OutboxRecord, RecordSpan> spans;

        Call(@Nullable Span step, List<OutboxRecord> records, Instant now) {
            this.records = List.copyOf(records);
            this.spans = new IdentityHashMap<>(2 * records.size());
            var fetch = step == null ? Context.current() : Context.root().with(step);
            for (var record : records) {
                var stored = propagator.extract(Context.root(), record.headers(), HEADERS);
                boolean restored = Span.fromContext(stored).getSpanContext().isValid();
                spans.put(record, new RecordSpan(record, restored ? stored : fetch,
                        restored && step != null ? step.getSpanContext() : null, now));
            }
        }

        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            var span = spans.get(record);
            if (span == null) {
                return call.call();
            }
            try (var scope = span.context().makeCurrent()) {
                return call.call();
            }
        }

        @Override
        public void recordFinished(OutboxRecord record, Outcome outcome) {
            var span = spans.get(record);
            if (span != null) { // a record not of this call: not ours to end
                span.end(outcome);
            }
        }

        @Override
        public void finished(List<Outcome> outcomes) {
            for (int i = 0; i < records.size() && i < outcomes.size(); i++) {
                var record = records.get(i);
                var outcome = outcomes.get(i);
                spans.get(record).end(outcome);
                if (!(outcome instanceof Outcome.NotAttempted)) {
                    var type = errorType(outcome);
                    meters.add(DELIVERED, type == null ? Map.of(DESTINATION.getKey(), record.topic())
                            : Map.of(DESTINATION.getKey(), record.topic(), ERROR_TYPE.getKey(), type));
                }
            }
        }

        @Override
        public void failed(Throwable error) {
            spans.values().forEach(s -> s.fail(error));
        }
    }

    /**
     * One record's {@code deliver} span, started the first time the handler enters its scope or its end comes, with
     * the call's start as its own: a record the handler never attempted gets none. The first end wins; a report may
     * race the call's end on the relay thread.
     */
    private final class RecordSpan {

        private final OutboxRecord record;
        private final Context parent;
        @Nullable
        private final SpanContext link;
        private final Instant start;
        @Nullable
        private Span span;
        private boolean ended;

        RecordSpan(OutboxRecord record, Context parent, @Nullable SpanContext link, Instant start) {
            this.record = record;
            this.parent = parent;
            this.link = link;
            this.start = start;
        }

        synchronized Context context() {
            return parent.with(span());
        }

        synchronized void end(Outcome outcome) {
            if (ended) {
                return;
            }
            ended = true;
            if (span == null && outcome instanceof Outcome.NotAttempted) {
                return;
            }
            var s = span();
            var type = errorType(outcome);
            if (type != null) {
                s.setAttribute(ERROR_TYPE, type).setStatus(StatusCode.ERROR);
            }
            s.end();
        }

        synchronized void fail(Throwable error) {
            if (ended) {
                return;
            }
            ended = true;
            if (span != null) {
                span.setAttribute(ERROR_TYPE, error.getClass().getName()).setStatus(StatusCode.ERROR)
                        .recordException(error);
                span.end();
            }
        }

        private Span span() {
            if (span == null) {
                var builder = common(tracer.spanBuilder("deliver " + record.topic()), "deliver", "process")
                        .setSpanKind(SpanKind.CONSUMER).setParent(parent).setStartTimestamp(start)
                        .setAttribute(DESTINATION, record.topic())
                        .setAttribute(RECORD_ID, record.messageId().toString())
                        .setAttribute(PARTITION, Integer.toString(record.shard()));
                if (record.attempts() >= 1) {
                    builder.setAttribute(RESEND_COUNT, (long) record.attempts());
                }
                if (link != null) {
                    builder.addLink(link);
                }
                span = builder.startSpan();
            }
            return span;
        }
    }

    @Nullable
    private static String errorType(Outcome outcome) {
        if (outcome instanceof Outcome.Retry r) {
            return r.error().getClass().getName();
        }
        if (outcome instanceof Outcome.Permanent p) {
            return p.error().getClass().getName();
        }
        return null;
    }

    // queue

    @Override
    public void queueState(List<ShardQueue> shards) {
        shards.forEach(q -> meters.queues.put(q.shard(), q));
    }

    private SpanBuilder common(SpanBuilder builder, String operation, String type) {
        return builder.setAttribute(SYSTEM, SYSTEM_NAME).setAttribute(OPERATION_NAME, operation)
                .setAttribute(OPERATION_TYPE, type).setAttribute(COLLECTION, table);
    }

    /**
     * Plain in-memory meters: counters by name and attributes, histogram values by name, and the last queue state per
     * shard, read as its pending and oldest-age gauges.
     */
    static final class Meters {

        private final Map<Meter, LongAdder> counters = new ConcurrentHashMap<>();
        private final Map<String, List<Long>> histograms = new ConcurrentHashMap<>();
        final Map<Integer, ShardQueue> queues = new ConcurrentHashMap<>();

        record Meter(String name, Map<String, String> attributes) {}

        void add(String name, Map<String, String> attributes) {
            counters.computeIfAbsent(new Meter(name, Map.copyOf(attributes)), m -> new LongAdder()).increment();
        }

        void record(String name, long value) {
            histograms.computeIfAbsent(name, n -> new CopyOnWriteArrayList<>()).add(value);
        }

        /** The count of the counter with exactly these attributes, as name/value pairs; 0 when none. */
        long count(String name, String... attributes) {
            var map = new LinkedHashMap<String, String>();
            for (int i = 0; i < attributes.length; i += 2) {
                map.put(attributes[i], attributes[i + 1]);
            }
            var counter = counters.get(new Meter(name, Map.copyOf(map)));
            return counter == null ? 0 : counter.sum();
        }

        List<Long> histogram(String name) {
            return List.copyOf(histograms.getOrDefault(name, List.of()));
        }

        /** The pending gauge of {@code shard}, NaN before any queue state. */
        double pending(int shard) {
            var q = queues.get(shard);
            return q == null ? Double.NaN : q.pending();
        }

        /** The oldest-age gauge of {@code shard}, in seconds, NaN before any queue state. */
        double oldestSeconds(int shard) {
            var q = queues.get(shard);
            return q == null ? Double.NaN : q.oldestSeconds();
        }
    }
}
