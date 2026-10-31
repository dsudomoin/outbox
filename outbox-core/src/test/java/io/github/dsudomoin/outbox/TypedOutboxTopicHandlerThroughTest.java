package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * {@code via} is a second instance of the handler's class with other topics: it stands in for a proxy whose fields are
 * not the handler's.
 */
class TypedOutboxTopicHandlerThroughTest {

    @OutboxContract(2)
    record V2(String text) {}

    private static final OutboxDeserializer<V2> TEXT = (topic, payload) -> new V2(new String(payload, UTF_8));
    private static final OutboxDeserializer<V2> NEVER = (topic, payload) -> {
        throw new AssertionError("via's deserializer called");
    };

    @Test
    void topicsAndContractVersionComeFromHandler() {
        var handler = new Recording(Set.of("audit"), TEXT);
        var through = TypedOutboxTopicHandler.calledThrough(handler, new Recording(Set.of("other"), NEVER));

        assertThat(through).isInstanceOf(TypedOutboxTopicHandler.class);
        assertThat(through.topics()).isEqualTo(handler.topics());
        assertThat(through.contractVersion()).isEqualTo(handler.contractVersion()).isEqualTo(2);
    }

    @Test
    void hooksRunOnVia() throws Exception {
        var handler = new Recording(Set.of("audit"), TEXT);
        var via = new Recording(Set.of("other"), NEVER);
        var through = TypedOutboxTopicHandler.calledThrough(handler, via);
        var tombstone = record(null);

        through.handle(record("a"));
        through.handle(tombstone);

        assertThat(via.calls).containsExactly(new V2("a"), tombstone);
        assertThat(handler.calls).isEmpty();
    }

    @Test
    void decodeFailuresUnchanged() {
        var handler = new Recording(Set.of("audit"), (topic, payload) -> {
            throw new IllegalStateException("bad");
        });
        var through = TypedOutboxTopicHandler.calledThrough(handler, new Recording(Set.of("other"), NEVER));
        var newer = record("a").withHeaders(List.of(new OutboxHeader("outbox-contract", "3".getBytes(US_ASCII))));

        assertThatThrownBy(() -> through.handle(newer)).isInstanceOf(OutboxWaitException.class);
        assertThatThrownBy(() -> through.handle(record("a")))
                .isInstanceOf(OutboxPermanentException.class)
                .hasMessageContaining(": " + Recording.class.getName() + " cannot decode its payload");
    }

    @Test
    void handleBatchOverrideGoesToVia() {
        var via = new Batching(Set.of("other"), NEVER);
        var through = TypedOutboxTopicHandler.calledThrough(new Batching(Set.of("audit"), TEXT), via);
        var records = List.of(record("a"));

        assertThat(through.handleBatch(records)).isSameAs(via.outcomes);
        assertThat(via.batches).containsExactly(records);
    }

    @Test
    void defaultBatchLoopCallsViaHookPerRecord() {
        var via = new Recording(Set.of("other"), NEVER);
        var through = TypedOutboxTopicHandler.calledThrough(new Recording(Set.of("audit"), TEXT), via);

        assertThat(through.handleBatch(List.of(record("a"), record("b"))))
                .containsExactly(Outcome.delivered(), Outcome.delivered());
        assertThat(via.calls).containsExactly(new V2("a"), new V2("b"));
    }

    @Test
    void toStringIsHandlers() {
        var handler = new Recording(Set.of("audit"), TEXT);

        assertThat(TypedOutboxTopicHandler.calledThrough(handler, new Recording(Set.of("other"), NEVER)))
                .hasToString(handler.toString());
    }

    @Test
    void noHandlerMessageNamesBothFrameworks() {
        assertThatThrownBy(() -> OutboxRuntime.builder(new TestOutboxConfig(), new NoTransactions(), null).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outbox-kafka-spring-boot")
                .hasMessageContaining("KafkaOutboxModule");
    }

    private static OutboxRecord record(@Nullable String payload) {
        return new OutboxRecord(1, UUID.randomUUID(), "audit", null, payload == null ? null : payload.getBytes(UTF_8),
                List.of(), Instant.EPOCH, 0, 0);
    }

    static class Recording extends TypedOutboxTopicHandler<V2> {

        final List<Object> calls = new ArrayList<>();

        Recording(Set<String> topics, OutboxDeserializer<V2> deserializer) {
            super(topics, deserializer);
        }

        @Override
        protected void handle(V2 value, OutboxRecord record) {
            calls.add(value);
        }

        @Override
        protected void handleTombstone(OutboxRecord record) {
            calls.add(record);
        }
    }

    static final class Batching extends Recording {

        final List<Outcome> outcomes = List.of(Outcome.notAttempted());
        final List<List<OutboxRecord>> batches = new ArrayList<>();

        Batching(Set<String> topics, OutboxDeserializer<V2> deserializer) {
            super(topics, deserializer);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            batches.add(records);
            return outcomes;
        }
    }

    /** Never asked: the build fails before any database access. */
    private static final class NoTransactions implements OutboxTransactions {

        @Override
        public Connection newConnection() {
            throw new AssertionError("no database access expected");
        }

        @Override
        public Connection currentConnection() {
            throw new AssertionError("no database access expected");
        }

        @Override
        public void afterCommit(Runnable action) {
            throw new AssertionError("no transaction expected");
        }

        @Override
        public String transactionHint() {
            return "a test transaction";
        }
    }
}
