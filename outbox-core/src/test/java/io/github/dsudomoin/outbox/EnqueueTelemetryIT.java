package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.testing.RecordingHandler;
import io.github.dsudomoin.outbox.testing.TestDataSources;
import io.github.dsudomoin.outbox.testing.TestOutboxConfig;
import io.github.dsudomoin.outbox.testing.TestSql;
import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The enqueue hooks of {@code send} and {@code sendAll}: a handle per message with its record id and shard, the
 * insert in its scope, and {@code enqueued} only after the business transaction commits.
 */
class EnqueueTelemetryIT {

    private static final OutboxHeader TRACE = new OutboxHeader("trace", bytes("t1"));

    private final JdbcTransactions transactions = new JdbcTransactions(TestDataSources.newDataSource());
    private final TestOutboxConfig config = new TestOutboxConfig().relayEnabled(false);
    private final RecordingTelemetry telemetry = new RecordingTelemetry();
    @Nullable
    private OutboxRuntime runtime;

    @AfterEach
    void close() {
        if (runtime != null) {
            runtime.close();
        }
    }

    @Test
    void sendCallsMessageWithItsIdAndShard() throws Exception {
        var outbox = start(telemetry, transactions);

        UUID id = transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "k1", bytes("p"))));

        assertThat(telemetry.messages).hasSize(1);
        var m = telemetry.messages.get(0);
        assertThat(m.id).isEqualTo(id);
        assertThat(m.topic).isEqualTo("orders");
        assertThat(m.shard).isEqualTo(storedShard(id));
        assertThat(telemetry.events).containsSubsequence("message orders", "inserted orders");
        assertThat(OutboxRuntimeIT.headers(transactions, config, id)).as("the headers Message.headers returned")
                .containsExactly(TRACE);
    }

    @Test
    void sendAllCallsMessageForEachInOrder() throws Exception {
        var outbox = start(telemetry, transactions);
        var messages = List.of(OutboxMessage.of("orders", "k1", bytes("a")), OutboxMessage.of("audit", bytes("b")),
                OutboxMessage.of("orders", "k2", bytes("c")));

        List<UUID> ids = transactions.inTx(c -> outbox.sendAll(messages));

        assertThat(telemetry.messages).extracting(m -> m.id).containsExactlyElementsOf(ids);
        assertThat(telemetry.messages).extracting(m -> m.topic).containsExactly("orders", "audit", "orders");
        for (var m : telemetry.messages) {
            assertThat(m.shard).as("the stored shard of %s", m.id).isEqualTo(storedShard(m.id));
            assertThat(m.inserted).isTrue();
            assertThat(OutboxRuntimeIT.headers(transactions, config, m.id)).containsExactly(TRACE);
        }
    }

    @Test
    void insertRunsInsideTheMessageScope() throws Exception {
        telemetry.countInScope = true;
        var outbox = start(telemetry, transactions);

        UUID id = transactions.inTx(c -> outbox.send(OutboxMessage.of("orders", "k1", bytes("p"))));

        var m = telemetry.messages.get(0);
        assertThat(m.id).isEqualTo(id);
        assertThat(m.scopeCalls).as("the scope's callable ran once").isEqualTo(1);
        assertThat(m.beforeInsert).isZero();
        assertThat(m.afterInsert).isOne();
        assertThat(telemetry.events).containsSubsequence("scope returned orders", "inserted orders");
    }

    @Test
    void failedInsertReachesTheMessage() throws Exception {
        var outbox = start(telemetry, transactions);
        refuseTopicBoom();

        assertThatThrownBy(() -> transactions.inTx(c -> outbox.send(OutboxMessage.of("boom", "k", bytes("p")))))
                .isInstanceOf(OutboxSqlException.class)
                .hasMessageContaining("boom refused")
                .satisfies(e -> {
                    assertThat(telemetry.messages).hasSize(1);
                    assertThat(telemetry.messages.get(0).failure).isSameAs(e);
                });
        assertThat(telemetry.events).containsExactly("enqueueStarted", "message boom", "enqueue headers",
                "message headers boom", "message failed boom", "enqueue failed");
        assertThat(telemetry.enqueued).isEmpty();
    }

    @Test
    void failedSendAllReachesEveryMessage() throws Exception {
        var outbox = start(telemetry, transactions);
        refuseTopicBoom();
        var messages = List.of(OutboxMessage.of("orders", "k1", bytes("a")), OutboxMessage.of("boom", bytes("b")),
                OutboxMessage.of("orders", "k2", bytes("c")));

        assertThatThrownBy(() -> transactions.inTx(c -> outbox.sendAll(messages)))
                .isInstanceOf(OutboxSqlException.class)
                .hasMessageContaining("boom refused")
                .satisfies(e -> assertThat(telemetry.messages).hasSize(3)
                        .allSatisfy(m -> assertThat(m.failure).isSameAs(e)));

        assertThat(telemetry.events).as("each message's handle before its headers, as in send").containsExactly(
                "enqueueStarted",
                "message orders", "enqueue headers", "message headers orders",
                "message boom", "enqueue headers", "message headers boom",
                "message orders", "enqueue headers", "message headers orders",
                "message failed orders", "message failed boom", "message failed orders",
                "enqueue failed");
        assertThat(telemetry.messages).noneMatch(m -> m.inserted);
        assertThat(telemetry.enqueued).isEmpty();
    }

    @Test
    void enqueuedOnlyAfterCommit() {
        var outbox = start(telemetry, transactions);

        transactions.inTx(c -> {
            outbox.send(OutboxMessage.of("orders", "k1", bytes("a")));
            outbox.sendAll(List.of(OutboxMessage.of("audit", bytes("b")), OutboxMessage.of("orders", bytes("c"))));
            assertThat(telemetry.enqueued).as("not before the commit").isEmpty();
            return null;
        });

        assertThat(telemetry.enqueued).containsExactly("orders", "audit", "orders");
    }

    @Test
    void enqueuedNeverForARollback() {
        var outbox = start(telemetry, transactions);

        assertThatThrownBy(() -> transactions.inTx(c -> {
            outbox.send(OutboxMessage.of("orders", "k1", bytes("a")));
            outbox.send(OutboxMessage.of("orders", bytes("b")));
            throw new IllegalStateException("rollback");
        })).hasMessage("rollback");

        assertThat(telemetry.messages).hasSize(2);
        assertThat(telemetry.enqueued).isEmpty();
    }

    @Test
    void foreignConnectionCountsAtInsert() throws Exception {
        var outbox = start(telemetry, transactions);

        try (var other = transactions.newConnection()) {
            other.setAutoCommit(false);
            outbox.send(other, OutboxMessage.of("orders", "k1", bytes("a")));
            assertThat(telemetry.enqueued).as("right after the call returns").containsExactly("orders");
            other.rollback();
        }

        assertThat(telemetry.enqueued).containsExactly("orders");
    }

    @Test
    void noneRegistersNoExtraAfterCommitAction() {
        var recording = new RecordingTransactions(transactions);
        var outbox = start(OutboxTelemetry.NONE, recording);

        for (int tx = 0; tx < 2; tx++) {
            recording.registered.clear();
            recording.each.clear();
            sendFourInThreeCalls(outbox);

            Set<Runnable> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
            distinct.addAll(recording.registered);
            assertThat(distinct).as("the wake-up hook only").hasSize(1);
            assertThat(recording.each).as("no counting action").isEmpty();
        }
    }

    @Test
    void countingRegistersOneActionPerCallThroughAfterCommitEach() {
        var recording = new RecordingTransactions(transactions);
        var outbox = start(telemetry, recording);

        sendFourInThreeCalls(outbox);

        Set<Runnable> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        distinct.addAll(recording.registered);
        assertThat(distinct).as("the wake-up hook only").hasSize(1);
        assertThat(recording.each).as("one counting action per call").hasSize(3).doesNotContainAnyElementsOf(distinct);
        assertThat(telemetry.enqueued).containsExactly("orders", "orders", "orders", "audit");
    }

    private void sendFourInThreeCalls(Outbox outbox) {
        transactions.inTx(c -> {
            outbox.send(OutboxMessage.of("orders", "k1", bytes("a")));
            outbox.send(OutboxMessage.of("orders", bytes("b")));
            outbox.sendAll(List.of(OutboxMessage.of("orders", "k2", bytes("c")), OutboxMessage.of("audit",
                    bytes("d"))));
            return null;
        });
    }

    /** A {@code BEFORE INSERT} trigger on the outbox table that raises for topic {@code boom}. */
    private void refuseTopicBoom() throws SQLException {
        try (var c = transactions.newConnection()) {
            var function = config.table() + "_boom";
            TestSql.execute(c, "create function " + function + "() returns trigger language plpgsql as $$ begin"
                    + " if new.topic = 'boom' then raise exception 'boom refused'; end if; return new; end $$");
            TestSql.execute(c, "create trigger boom before insert on " + config.table()
                    + " for each row execute function " + function + "()");
        }
    }

    private Outbox start(OutboxTelemetry telemetry, OutboxTransactions transactions) {
        var r = OutboxRuntime.builder(config, transactions, new RecordingHandler()).telemetry(telemetry).build();
        runtime = r;
        r.start();
        return r.outbox();
    }

    private int storedShard(UUID id) throws SQLException {
        try (var c = transactions.newConnection();
                var ps = c.prepareStatement("select shard from " + config.table() + " where message_id = ?")) {
            ps.setObject(1, id);
            try (var rs = ps.executeQuery()) {
                assertThat(rs.next()).as("row of %s", id).isTrue();
                return rs.getInt(1);
            }
        }
    }

    private long countOf(UUID id) throws SQLException {
        var c = transactions.currentConnection();
        try (var ps = c.prepareStatement("select count(*) from " + config.table() + " where message_id = ?")) {
            ps.setObject(1, id);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static byte[] bytes(String s) {
        return s.getBytes(UTF_8);
    }

    /** One message handle, as the telemetry saw it. */
    private static final class Seen {
        final UUID id;
        final String topic;
        final int shard;
        int scopeCalls;
        long beforeInsert = -1;
        long afterInsert = -1;
        boolean inserted;
        @Nullable
        Throwable failure;

        Seen(UUID id, String topic, int shard) {
            this.id = id;
            this.topic = topic;
            this.shard = shard;
        }
    }

    /** Every enqueue hook in order; each message gets the {@code trace} header. */
    private final class RecordingTelemetry implements OutboxTelemetry {

        final List<String> events = new CopyOnWriteArrayList<>();
        final List<Seen> messages = new CopyOnWriteArrayList<>();
        final List<String> enqueued = new CopyOnWriteArrayList<>();
        volatile boolean countInScope;

        @Override
        public Enqueue enqueueStarted() {
            events.add("enqueueStarted");
            return new Enqueue() {
                @Override
                public Message message(OutboxMessage message, UUID recordId, int shard) {
                    var seen = new Seen(recordId, message.topic(), shard);
                    messages.add(seen);
                    events.add("message " + message.topic());
                    return new Message() {
                        @Override
                        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
                            events.add("message headers " + seen.topic);
                            var added = new ArrayList<>(headers);
                            added.add(TRACE);
                            return added;
                        }

                        @Override
                        public <T> T inScope(Callable<T> insert) throws Exception {
                            if (countInScope) {
                                seen.beforeInsert = countOf(seen.id);
                            }
                            seen.scopeCalls++;
                            var result = insert.call();
                            if (countInScope) {
                                seen.afterInsert = countOf(seen.id);
                            }
                            events.add("scope returned " + seen.topic);
                            return result;
                        }

                        @Override
                        public void inserted() {
                            seen.inserted = true;
                            events.add("inserted " + seen.topic);
                        }

                        @Override
                        public void failed(Throwable error) {
                            seen.failure = error;
                            events.add("message failed " + seen.topic);
                        }
                    };
                }

                @Override
                public List<OutboxHeader> headers(List<OutboxHeader> headers) {
                    events.add("enqueue headers");
                    return headers;
                }

                @Override
                public void failed(Throwable error) {
                    events.add("enqueue failed");
                }
            };
        }

        @Override
        public void enqueued(String topic) {
            enqueued.add(topic);
        }
    }

    /** {@link JdbcTransactions} that records every after-commit registration, {@code afterCommitEach} apart. */
    private static final class RecordingTransactions implements OutboxTransactions {

        final List<Runnable> registered = new CopyOnWriteArrayList<>();
        final List<Runnable> each = new CopyOnWriteArrayList<>();
        private final JdbcTransactions delegate;

        RecordingTransactions(JdbcTransactions delegate) {
            this.delegate = delegate;
        }

        @Nullable
        @Override
        public Connection currentConnection() {
            return delegate.currentConnection();
        }

        @Override
        public void afterCommit(Runnable action) {
            registered.add(action);
            delegate.afterCommit(action);
        }

        @Override
        public void afterCommitEach(Runnable action) throws SQLException {
            each.add(action);
            delegate.afterCommitEach(action);
        }

        @Override
        public String transactionHint() {
            return delegate.transactionHint();
        }

        @Override
        public Connection newConnection() throws SQLException {
            return delegate.newConnection();
        }
    }
}
