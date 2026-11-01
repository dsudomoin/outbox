package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.Outcome;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A plain handler bean behind a Spring AOP proxy, called through the proxy so that its advice runs: {@code handle} on
 * the proxy, and {@code handleBatch} on the proxy when the bean's class overrides it, else the core's default loop
 * over the proxy's {@code handle}. Its topics are the target's, read once. Both kinds in one class: the runtime takes
 * it as the fallback {@link OutboxHandler} or as an {@link OutboxTopicHandler} route.
 */
final class ProxiedHandler implements OutboxHandler, OutboxTopicHandler {

    private final String description;
    private final Set<String> topics;
    private final OutboxHandler sequential;
    @Nullable
    private final Function<List<OutboxRecord>, List<Outcome>> batch;

    /** {@code batch}: the proxy's {@code handleBatch} when the bean's class overrides it, else null. */
    ProxiedHandler(String description, Set<String> topics, OutboxHandler handle,
            @Nullable Function<List<OutboxRecord>, List<Outcome>> batch) {
        this.description = description;
        this.topics = topics;
        this.sequential = handle;
        this.batch = batch;
    }

    @Override
    public Set<String> topics() {
        return topics;
    }

    @Override
    public void handle(OutboxRecord record) throws Exception {
        sequential.handle(record);
    }

    @Override
    public List<Outcome> handleBatch(List<OutboxRecord> records) {
        return batch != null ? batch.apply(records) : sequential.handleBatch(records);
    }

    @Override
    public String toString() {
        return description;
    }
}
