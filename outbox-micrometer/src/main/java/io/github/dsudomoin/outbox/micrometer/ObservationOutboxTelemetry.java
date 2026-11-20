package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.DoubleSupplier;

/**
 * The outbox on Micrometer Observation, in four observations, each with {@code messaging.system=outbox}:
 * <ul>
 *   <li>{@code outbox.enqueue}, one per message a {@code send}/{@code sendAll} call writes, a sender observation of
 *   kind PRODUCER whose trace context the message stores (unless it carries its own); current while the message's own
 *   INSERT runs ({@code sendAll} writes all in one statement, outside any of them).</li>
 *   <li>{@code outbox.relay.step}, one per relay step that finds work, a client sender observation (kind CLIENT with
 *   the application's propagating sender handler), current on the thread that steps (a relay worker, or the caller
 *   of {@code OutboxRelay.drain()}) from the first shard with work until the step's transaction ended.</li>
 *   <li>{@code outbox.relay.settle}, the step's writes from its last handler call to its commit or rollback, a client
 *   observation under the step's, current meanwhile; the step ends after it.</li>
 *   <li>{@code outbox.deliver}, one per delivered record whose stored trace is sampled (every record with
 *   {@link Builder#observeUnsampled}), a receiver observation of kind CONSUMER parented by that stored context, whose
 *   own context the record handed to the handler carries. It ends as soon as the handler reports the record's
 *   outcome ({@code OutboxHandler.recordFinished}), else at the end of the call; the first end wins.</li>
 * </ul>
 * <p>A record stored without trace context (written without this telemetry) gets the step's span, current then, as
 * its parent. Register {@link OutboxDeliveryTracingObservationHandler} for the delivery spans to link to their
 * step's span. {@link Builder#tracing} lets the handed records carry the delivery span; without it they keep their headers.
 * The other events and the gauges are {@link OutboxMetrics}' meters. Names and key values come from the conventions;
 * meter names and tags can be changed with a {@code MeterFilter}.
 */
public final class ObservationOutboxTelemetry implements OutboxTelemetry {

    private final ObservationRegistry observations;
    private final OutboxMetrics metrics;
    @Nullable
    private final TraceInjector injector;
    @Nullable
    private final OutboxEnqueueObservationConvention enqueueConvention;
    @Nullable
    private final OutboxDeliveryObservationConvention deliveryConvention;
    @Nullable
    private final OutboxStepObservationConvention stepConvention;
    @Nullable
    private final OutboxSettleObservationConvention settleConvention;
    private final boolean observeUnsampled;
    /** Stateless: each message is observed on its own. */
    private final Enqueue enqueue = new ObservedEnqueue();

    private ObservationOutboxTelemetry(Builder builder) {
        this.observations = builder.observations;
        this.metrics = builder.metrics;
        this.injector = builder.injector;
        this.enqueueConvention = builder.enqueueConvention;
        this.deliveryConvention = builder.deliveryConvention;
        this.stepConvention = builder.stepConvention;
        this.settleConvention = builder.settleConvention;
        this.observeUnsampled = builder.observeUnsampled;
    }

    public static Builder builder(ObservationRegistry observations, OutboxMetrics metrics) {
        return new Builder(observations, metrics);
    }

    /** Starts nothing: each message of the call is observed on its own, see {@link Enqueue#message}. */
    @Override
    public Enqueue enqueueStarted() {
        return enqueue;
    }

    @Override
    public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
        return deliveryStarted(RelayStep.NONE, records, now);
    }

    @Override
    public Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, Instant now) {
        var stepObservation = step instanceof ObservedStep s && !s.observation.isNoop() ? s.observation : null;
        var started = new Observation[records.size()];
        var contexts = new OutboxDeliveryContext[records.size()];
        try {
            for (int i = 0; i < started.length; i++) {
                if (!observeUnsampled && unsampled(records.get(i).headers())) {
                    continue; // not observed: no observation, no context
                }
                var context = new OutboxDeliveryContext(records.get(i), now, stepObservation);
                contexts[i] = context;
                // stored before it starts: a handler that throws in onStart may have started a meter sample already
                started[i] = Observation.createNotStarted(deliveryConvention,
                        DefaultOutboxDeliveryObservationConvention.INSTANCE, () -> context, observations);
                started[i].start();
            }
        } catch (Throwable e) {
            // the caller gets no Delivery to stop them with: stop the ones already started, or they never end
            for (var observation : started) {
                if (observation != null) {
                    end(observation, e, e);
                }
            }
            throw e;
        }
        return new ObservedDelivery(records, started, contexts);
    }

    /**
     * Starts the observation, or stops it when starting throws: Micrometer notifies the handlers' {@code onStart} in
     * turn without a catch, so the ones before a handler that throws have started (a meter handler its active sample),
     * and the caller gets nothing to stop it with.
     */
    private static void start(Observation observation) {
        try {
            observation.start();
        } catch (Throwable e) {
            end(observation, e, e);
            throw e;
        }
    }

    /**
     * Records {@code error} (when not null) and stops the observation, which a convention, filter or handler that
     * throws cannot prevent unless it throws in {@code stop()} itself. Returns {@code first}, or the failure when
     * there was none; a later failure is added to {@code first} as suppressed.
     */
    @Nullable
    private static Throwable end(Observation observation, @Nullable Throwable error, @Nullable Throwable first) {
        try {
            try {
                if (error != null) {
                    observation.error(error);
                }
            } finally {
                observation.stop();
            }
        } catch (Throwable e) {
            if (first == null) {
                return e;
            }
            if (e != first) { // a handler may rethrow the error it is given
                first.addSuppressed(e);
            }
        }
        return first;
    }

    private static void rethrow(@Nullable Throwable failure) {
        if (failure instanceof RuntimeException e) {
            throw e;
        } else if (failure instanceof Error e) {
            throw e;
        } else if (failure != null) {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Whether the last {@code traceparent} header is a well-formed W3C trace context
     * ({@link TraceHeaders#wellFormedTraceparent}) whose sampled flag (bit 0 of the trace flags) is clear.
     */
    private static boolean unsampled(List<OutboxHeader> headers) {
        var v = TraceHeaders.last(headers, TraceHeaders.TRACEPARENT);
        return TraceHeaders.wellFormedTraceparent(v) && (TraceHeaders.traceFlags(v) & 1) == 0;
    }

    /**
     * Opens the step observation's scope on the thread that steps; the step closes it there before it stops. A
     * Micrometer scope makes itself current before it notifies the handlers and restores the previous one only after
     * they are notified on close, so a handler that throws there would leave it current: the thread's previous scope is
     * restored either way, see {@link #restoreScope}.
     */
    @Override
    public RelayStep stepStarted(List<Integer> shards) {
        var context = new OutboxStepContext(shards);
        var observation = Observation.createNotStarted(stepConvention, DefaultOutboxStepObservationConvention.INSTANCE,
                () -> context, observations);
        start(observation);
        var previous = observations.getCurrentObservationScope();
        return new ObservedStep(observation, context, openScope(observation, previous), previous);
    }

    /**
     * Opens the scope of {@code observation}, started; when that throws, restores {@code previous} and stops the
     * observation, which the caller gets nothing to stop with.
     */
    private Observation.Scope openScope(Observation observation, @Nullable Observation.Scope previous) {
        try {
            return observation.openScope();
        } catch (Throwable e) {
            try {
                restoreScope(observations, previous);
            } finally {
                end(observation, null, e);
            }
            throw e;
        }
    }

    /**
     * Records {@code error} (when not null), closes {@code scope}, makes {@code previous} the thread's scope again and
     * stops the observation: the scope is closed and the observation stopped even when recording the error fails, so
     * none stays current on the thread, none stays running.
     */
    private void endScoped(Observation observation, Observation.Scope scope, @Nullable Observation.Scope previous,
            @Nullable Throwable error) {
        try {
            if (error != null) {
                observation.error(error);
            }
        } finally {
            try {
                try {
                    scope.close();
                } finally {
                    restoreScope(observations, previous);
                }
            } finally {
                observation.stop();
            }
        }
    }

    /**
     * Makes {@code previous} the thread's scope again. A scope that a handler left current, by throwing while it opened
     * or closed, is reset first: the tracing handler then closes the tracer's scope as well, which would otherwise keep
     * that span current and parent whatever the thread observes next. A reset leaves no span current in the tracer,
     * so a {@code previous} scope's span is not current there again until that scope closes.
     */
    static void restoreScope(ObservationRegistry observations, @Nullable Observation.Scope previous) {
        var left = observations.getCurrentObservationScope();
        try {
            if (left != null && left != previous) {
                left.reset();
            }
        } finally {
            observations.setCurrentObservationScope(previous);
        }
    }

    /** On a handler or dead-letter hook thread, which the library owns: no observation stays current there. */
    @Override
    public void releaseThread() {
        observations.setCurrentObservationScope(null);
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

    /** Observes each message of a call on its own; the call itself starts and ends nothing. */
    private final class ObservedEnqueue implements Enqueue {
        @Override
        public Message message(OutboxMessage message, UUID recordId, int shard) {
            var context = new OutboxEnqueueContext(message, recordId, shard);
            var observation = Observation.createNotStarted(enqueueConvention,
                    DefaultOutboxEnqueueObservationConvention.INSTANCE, () -> context, observations);
            start(observation); // the current observation, the writer's, is its parent
            return new ObservedMessage(observation, context.getCarrier());
        }
    }

    /**
     * One message's observation: its injected trace headers stored unless the message has a header of one of their
     * names, current while its insert runs.
     */
    private final class ObservedMessage implements Message {

        private final Observation observation;
        private final List<OutboxHeader> injected;

        ObservedMessage(Observation observation, List<OutboxHeader> injected) {
            this.observation = observation;
            this.injected = injected;
        }

        /**
         * {@code headers} as they are when the message has a trace header of its own that gives it a parent (any
         * injected name but {@code baggage} and {@code tracestate}); else with the injected headers added, but for the
         * names the message already has.
         */
        @Override
        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
            if (injected.isEmpty()) {
                return headers;
            }
            for (var i : injected) {
                if (TraceHeaders.carriesParent(i.name()) && named(headers, i.name())) {
                    return headers;
                }
            }
            var all = new ArrayList<OutboxHeader>(headers.size() + injected.size());
            all.addAll(headers);
            for (var i : injected) {
                if (!named(headers, i.name())) {
                    all.add(i);
                }
            }
            return all;
        }

        private static boolean named(List<OutboxHeader> headers, String name) {
            for (var h : headers) {
                if (h.name().equals(name)) {
                    return true;
                }
            }
            return false;
        }

        /** The thread's previous scope is restored however it ends: see {@link #stepStarted}. */
        @Override
        public <T> T inScope(Callable<T> insert) throws Exception {
            var previous = observations.getCurrentObservationScope();
            try (var scope = observation.openScope()) {
                return insert.call();
            } finally {
                restoreScope(observations, previous);
            }
        }

        @Override
        public void inserted() {
            observation.stop();
        }

        @Override
        public void failed(Throwable error) {
            rethrow(end(observation, error, null));
        }
    }

    /** The step's observation, current on the thread that steps until its transaction ended. */
    private final class ObservedStep implements RelayStep {

        private final Observation observation;
        private final OutboxStepContext context;
        private final Observation.Scope scope;
        @Nullable
        private final Observation.Scope previous;

        ObservedStep(Observation observation, OutboxStepContext context, Observation.Scope scope,
                @Nullable Observation.Scope previous) {
            this.observation = observation;
            this.context = context;
            this.scope = scope;
            this.previous = previous;
        }

        @Override
        public void fetched(int shard, int records) {
            context.fetched(records);
        }

        /** A child of the step, current until it ends, on the same thread. */
        @Override
        public Settle settleStarted() {
            var settleContext = new OutboxSettleContext();
            var settle = Observation.createNotStarted(settleConvention,
                    DefaultOutboxSettleObservationConvention.INSTANCE, () -> settleContext, observations);
            if (!observation.isNoop()) {
                settle.parentObservation(observation);
            }
            start(settle);
            var before = observations.getCurrentObservationScope();
            return new ObservedSettle(settle, settleContext, openScope(settle, before), before);
        }

        @Override
        public void finished() {
            endScoped(observation, scope, previous, null);
        }

        @Override
        public void failed(Throwable error) {
            endScoped(observation, scope, previous, error);
        }
    }

    private final class ObservedSettle implements Settle {

        private final Observation observation;
        private final OutboxSettleContext context;
        private final Observation.Scope scope;
        @Nullable
        private final Observation.Scope previous;

        ObservedSettle(Observation observation, OutboxSettleContext context, Observation.Scope scope,
                @Nullable Observation.Scope previous) {
            this.observation = observation;
            this.context = context;
            this.scope = scope;
            this.previous = previous;
        }

        @Override
        public void committed(int records) {
            context.records(records);
            endScoped(observation, scope, previous, null);
        }

        @Override
        public void failed(int records, Throwable error) {
            context.records(records);
            endScoped(observation, scope, previous, error);
        }
    }

    /**
     * The observations of one handler call, in record order; started on the relay thread, each ended once: by the
     * handler's report on the handler thread ({@link #recordFinished}), or by the call's end on the relay thread,
     * whichever comes first ({@link OutboxDeliveryContext#takeEnd}). A record not observed
     * ({@link Builder#observeUnsampled}) has none: it is handed as stored and handled without a scope.
     */
    private final class ObservedDelivery implements Delivery {

        /** Null where a record is not observed. */
        private final Observation[] started;
        private final OutboxDeliveryContext[] contexts;
        /**
         * The index of each record the handler may get: the stored ones and the handed ones. Written on the relay
         * thread before the records reach the handler thread, only read after.
         */
        private final Map<OutboxRecord, Integer> byRecord;

        ObservedDelivery(List<OutboxRecord> records, Observation[] started, OutboxDeliveryContext[] contexts) {
            this.started = started;
            this.contexts = contexts;
            this.byRecord = new IdentityHashMap<>(2 * started.length); // sized once: stored and handed records
            for (int i = 0; i < started.length; i++) {
                if (started[i] != null) {
                    byRecord.put(records.get(i), i);
                }
            }
        }

        @Override
        public List<OutboxRecord> handed(List<OutboxRecord> records) {
            if (injector == null) {
                return records;
            }
            var handed = new ArrayList<OutboxRecord>(records.size());
            for (int i = 0; i < records.size(); i++) {
                if (started[i] == null) {
                    handed.add(records.get(i));
                    continue;
                }
                var record = injector.inject(observations, started[i], records.get(i));
                byRecord.put(record, i);
                handed.add(record);
            }
            return handed;
        }

        /** On the handler thread: the record's observation is the current one, its span the current span. */
        @Override
        public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
            var i = byRecord.get(record);
            if (i == null) {
                return call.call();
            }
            var previous = observations.getCurrentObservationScope(); // restored however it ends: see stepStarted
            try (var scope = started[i].openScope()) {
                return call.call();
            } finally {
                restoreScope(observations, previous);
            }
        }

        /** Ends the record's observation unless it has ended; a record not of this call, or not observed, is ignored. */
        @Override
        public void recordFinished(OutboxRecord record, Outcome outcome) {
            var i = byRecord.get(record);
            if (i != null) {
                rethrow(end(i, outcome, null, null));
            }
        }

        /** Every record's observation not ended yet is stopped even when another's fails; the first failure after. */
        @Override
        public void finished(List<Outcome> outcomes) {
            Throwable failure = null;
            for (int i = 0; i < started.length; i++) {
                if (started[i] != null) {
                    failure = end(i, outcomes.get(i), null, failure);
                }
            }
            rethrow(failure);
        }

        @Override
        public void failed(Throwable error) {
            Throwable failure = null;
            for (int i = 0; i < started.length; i++) {
                if (started[i] != null) {
                    failure = end(i, null, error, failure);
                }
            }
            rethrow(failure);
        }

        /**
         * Ends record {@code i}'s observation with {@code outcome}, or as failed with {@code error} when null, unless
         * another end took it: returns {@code first}, or the failure to stop.
         */
        @Nullable
        private Throwable end(int i, @Nullable Outcome outcome, @Nullable Throwable error,
                @Nullable Throwable first) {
            var context = contexts[i];
            if (!context.takeEnd()) {
                return first;
            }
            if (outcome != null) {
                context.outcome(outcome);
                error = outcome instanceof Outcome.Retry r ? r.error()
                        : outcome instanceof Outcome.Permanent p ? p.error() : null;
            } else {
                context.markFailed();
            }
            return ObservationOutboxTelemetry.end(started[i], error, first);
        }
    }

    public static final class Builder {

        private final ObservationRegistry observations;
        private final OutboxMetrics metrics;
        @Nullable
        private TraceInjector injector;
        @Nullable
        private OutboxEnqueueObservationConvention enqueueConvention;
        @Nullable
        private OutboxDeliveryObservationConvention deliveryConvention;
        @Nullable
        private OutboxStepObservationConvention stepConvention;
        @Nullable
        private OutboxSettleObservationConvention settleConvention;
        private boolean observeUnsampled;

        private Builder(ObservationRegistry observations, OutboxMetrics metrics) {
            this.observations = Objects.requireNonNull(observations, "observations");
            this.metrics = Objects.requireNonNull(metrics, "metrics");
        }

        /**
         * The records handed to the handler carry their delivery span, as {@code propagator} writes it (its fields
         * replace the stored trace headers). Needs micrometer-tracing on the classpath; without this call it is not
         * needed, and handed records keep their headers.
         */
        public Builder tracing(Tracer tracer, Propagator propagator) {
            this.injector = new TraceInjector(Objects.requireNonNull(tracer, "tracer"),
                    Objects.requireNonNull(propagator, "propagator"));
            return this;
        }

        public Builder enqueueConvention(OutboxEnqueueObservationConvention convention) {
            this.enqueueConvention = Objects.requireNonNull(convention, "convention");
            return this;
        }

        public Builder deliveryConvention(OutboxDeliveryObservationConvention convention) {
            this.deliveryConvention = Objects.requireNonNull(convention, "convention");
            return this;
        }

        public Builder settleConvention(OutboxSettleObservationConvention convention) {
            this.settleConvention = Objects.requireNonNull(convention, "convention");
            return this;
        }

        public Builder stepConvention(OutboxStepObservationConvention convention) {
            this.stepConvention = Objects.requireNonNull(convention, "convention");
            return this;
        }

        /**
         * Whether to observe the delivery of a record whose stored trace is not sampled; {@code false} by default, so
         * the delivery follows the stored trace's sampling decision, as a parent-based sampler would, and
         * {@code true} gives every delivered record an {@code outbox.deliver} observation. Not sampled means the
         * record's last stored {@code traceparent} header is a well-formed W3C trace context with the sampled flag
         * clear. Only W3C counts: a record with a sampled, missing or malformed {@code traceparent}, or with another
         * format's headers only, is always observed. A record not observed is handed to the handler with its stored
         * headers, so downstream consumers keep its trace, but the handler's code for it runs without that trace
         * context current: on a relay handler thread nothing is current unless something carries context into that
         * pool (an agent, say), and then it is the relay step's, which an instrumented client inside the handler would
         * write instead. The library's Kafka sender sends the stored headers either way. A meter handler's
         * {@code outbox.deliver} timer then counts only the observed records; the {@link OutboxMetrics} meters count
         * every record. Skipping cuts the per-record cost of Observation on the relay thread when most traces are not
         * sampled.
         */
        public Builder observeUnsampled(boolean observe) {
            this.observeUnsampled = observe;
            return this;
        }

        public ObservationOutboxTelemetry build() {
            return new ObservationOutboxTelemetry(this);
        }
    }
}
