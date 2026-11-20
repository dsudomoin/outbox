package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.micrometer.observation.Observation;
import io.micrometer.tracing.Link;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.TracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;

/**
 * The tracing handler of {@code outbox.deliver} observations: the span of a
 * {@link PropagatingReceiverTracingObservationHandler}, whose parent is a record's stored trace context, plus a link to
 * the span of the relay step that delivers the record. A record without a stored context the propagator can read has
 * the step's span, current then, as its parent, and no link. Readable means, as the getter reads the last header of a name: a well-formed W3C {@code traceparent}
 * when the propagator reads W3C trace context, a well-formed B3 single header or B3 trace and span ids when it reads
 * B3. {@code baggage} and {@code tracestate} alone carry no parent. A record stored in another format gets no link.
 * Register it before a plain receiver handler in a first-matching composite; it supports
 * {@link OutboxDeliveryContext} only.
 * <p>
 * It is no {@code PropagatingReceiverTracingObservationHandler} itself, so that a bean of it, or of a subclass, does
 * not stand in for one: a framework that registers its receiver handler only when the context has none keeps it for
 * every other receiver. To change the delivery span, override {@link #customizeDeliverySpan}.
 */
public class OutboxDeliveryTracingObservationHandler implements TracingObservationHandler<OutboxDeliveryContext> {

    private final PropagatingReceiverTracingObservationHandler<OutboxDeliveryContext> receiver;
    private final boolean w3c;
    /** A B3 propagator extracts the single and the multi header formats whichever it injects. */
    private final boolean b3;

    public OutboxDeliveryTracingObservationHandler(Tracer tracer, Propagator propagator) {
        this.receiver = new PropagatingReceiverTracingObservationHandler<>(tracer, propagator) {
            @Override
            public Span.Builder customizeExtractedSpan(OutboxDeliveryContext context, Span.Builder builder) {
                return customizeDeliverySpan(context, linkToStep(context, builder));
            }
        };
        var fields = propagator.fields();
        this.w3c = fields.contains(TraceHeaders.TRACEPARENT);
        this.b3 = fields.stream().anyMatch(f -> f.equals(TraceHeaders.B3) || f.startsWith("X-B3-"));
    }

    /**
     * Customises the delivery span before it starts, after its parent is extracted and the link to the step's span
     * added; the default returns {@code builder}.
     */
    protected Span.Builder customizeDeliverySpan(OutboxDeliveryContext context, Span.Builder builder) {
        return builder;
    }

    @Override
    public void onStart(OutboxDeliveryContext context) {
        receiver.onStart(context);
    }

    @Override
    public void onError(OutboxDeliveryContext context) {
        receiver.onError(context);
    }

    @Override
    public void onStop(OutboxDeliveryContext context) {
        receiver.onStop(context);
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
        return context instanceof OutboxDeliveryContext;
    }

    @Override
    public Tracer getTracer() {
        return receiver.getTracer();
    }

    private Span.Builder linkToStep(OutboxDeliveryContext context, Span.Builder builder) {
        var step = context.step();
        if (step == null || !storesContext(context.record())) {
            return builder;
        }
        TracingObservationHandler.TracingContext tracing =
                step.getContextView().get(TracingObservationHandler.TracingContext.class);
        var span = tracing == null ? null : tracing.getSpan();
        return span == null ? builder : builder.addLink(new Link(span.context()));
    }

    /** Whether the propagator extracts a parent from the record's headers, see the class doc. */
    private boolean storesContext(OutboxRecord record) {
        var headers = record.headers();
        return w3c && TraceHeaders.wellFormedTraceparent(TraceHeaders.last(headers, TraceHeaders.TRACEPARENT))
                || b3 && (TraceHeaders.wellFormedB3(TraceHeaders.last(headers, TraceHeaders.B3))
                        || TraceHeaders.wellFormedB3Ids(TraceHeaders.last(headers, TraceHeaders.B3_TRACE_ID),
                                TraceHeaders.last(headers, TraceHeaders.B3_SPAN_ID)));
    }
}
