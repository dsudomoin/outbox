package io.github.dsudomoin.outbox.micrometer;

import io.github.dsudomoin.outbox.OutboxRecord;
import io.micrometer.observation.Observation;
import io.micrometer.tracing.Link;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.TracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import java.util.Set;

/**
 * The tracing handler of {@code outbox.deliver} observations: {@link PropagatingReceiverTracingObservationHandler},
 * whose span a record's stored trace context parents, plus a link to the span of the relay step that delivers the
 * record. A record stored without trace context has the step's span, current then, as its parent, and no link. A
 * record counts as stored with context when it has a header of one of the propagator's fields. Register it before a
 * plain receiver handler in a first-matching composite; it supports {@link OutboxDeliveryContext} only.
 */
public class OutboxDeliveryTracingObservationHandler
        extends PropagatingReceiverTracingObservationHandler<OutboxDeliveryContext> {

    private final Set<String> fields;

    public OutboxDeliveryTracingObservationHandler(Tracer tracer, Propagator propagator) {
        super(tracer, propagator);
        this.fields = Set.copyOf(propagator.fields());
    }

    @Override
    public Span.Builder customizeExtractedSpan(OutboxDeliveryContext context, Span.Builder builder) {
        var step = context.step();
        if (step == null || !storesContext(context.record())) {
            return builder;
        }
        TracingObservationHandler.TracingContext tracing =
                step.getContextView().get(TracingObservationHandler.TracingContext.class);
        var span = tracing == null ? null : tracing.getSpan();
        return span == null ? builder : builder.addLink(new Link(span.context()));
    }

    @Override
    public boolean supportsContext(Observation.Context context) {
        return context instanceof OutboxDeliveryContext;
    }

    private boolean storesContext(OutboxRecord record) {
        for (var h : record.headers()) {
            if (h.value() != null && fields.contains(h.name())) {
                return true;
            }
        }
        return false;
    }
}
