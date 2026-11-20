package io.github.dsudomoin.outbox;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.dsudomoin.outbox.internal.RecordScope;
import io.github.dsudomoin.outbox.internal.TestHandlerCall;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** {@link OutboxHandler#holdsPermanent()} and what the default {@code handleBatch} does with it. */
class OutboxHandlerTest {

    private static final Duration BATCH_TIMEOUT = Duration.ofSeconds(30);

    @Test
    void holdsPermanentIsTrueOnlyOnTheThreadOfACallWhoseRelayHoldsPermanentFailures() throws Exception {
        var seen = new ArrayList<Boolean>();
        var otherThread = new CompletableFuture<Boolean>();
        TestHandlerCall.holdingPermanent(BATCH_TIMEOUT, () -> {
            seen.add(OutboxHandler.holdsPermanent());
            new Thread(() -> otherThread.complete(OutboxHandler.holdsPermanent())).start();
            return List.of();
        }).call();
        new TestHandlerCall(BATCH_TIMEOUT, () -> {
            seen.add(OutboxHandler.holdsPermanent());
            return List.of();
        }).call();

        assertThat(seen).containsExactly(true, false);
        assertThat(otherThread.get()).as("a thread the call started").isFalse();
        assertThat(OutboxHandler.holdsPermanent()).as("outside a relay call").isFalse();
    }

    @Test
    void theDefaultHandleBatchSkipsTheRestOfAKeyAfterAPermanentOnlyWhenItIsHeld() throws Exception {
        OutboxHandler handler = record -> {
            if (new String(record.payload(), UTF_8).equals("bad")) {
                throw new OutboxPermanentException("bad");
            }
        };
        var records = List.of(record(1, "k", "bad"), record(2, "k", "next"), record(3, "other", "ok"));

        var held = new AtomicReference<List<Outcome>>();
        TestHandlerCall.holdingPermanent(BATCH_TIMEOUT, () -> {
            held.set(handler.handleBatch(records));
            return List.of();
        }).call();
        var usual = new AtomicReference<List<Outcome>>();
        new TestHandlerCall(BATCH_TIMEOUT, () -> {
            usual.set(handler.handleBatch(records));
            return List.of();
        }).call();

        assertThat(held.get().get(0)).isInstanceOf(Outcome.Permanent.class);
        assertThat(held.get().subList(1, 3)).containsExactly(Outcome.notAttempted(), Outcome.delivered());
        assertThat(usual.get().get(0)).isInstanceOf(Outcome.Permanent.class);
        assertThat(usual.get().subList(1, 3)).containsExactly(Outcome.delivered(), Outcome.delivered());
    }

    /**
     * The relay turns a held Permanent into a Retry with its error before the call ends: the early report says the
     * same, so the telemetry ends the record as the relay settles it. Without holdPermanent the report is the
     * Permanent.
     */
    @Test
    void aHeldPermanentIsReportedAsTheRetryTheRelayMakesOfIt() throws Exception {
        var error = new OutboxPermanentException("bad");
        OutboxHandler handler = record -> {
            if (new String(record.payload(), UTF_8).equals("bad")) {
                throw error;
            }
        };
        var records = List.of(record(1, "k", "bad"), record(2, "k", "next"), record(3, "other", "ok"));
        var reported = new CopyOnWriteArrayList<Outcome>();
        var delivery = new OutboxTelemetry.Delivery() {
            @Override
            public void recordFinished(OutboxRecord record, Outcome outcome) {
                reported.add(outcome);
            }
        };

        TestHandlerCall.holdingPermanent(BATCH_TIMEOUT, () -> RecordScope.bound(delivery,
                () -> handler.handleBatch(records))).call();
        var held = List.copyOf(reported);
        reported.clear();
        new TestHandlerCall(BATCH_TIMEOUT, () -> RecordScope.bound(delivery, () -> handler.handleBatch(records)))
                .call();

        assertThat(held).hasSize(2);
        assertThat(held.get(0)).isInstanceOfSatisfying(Outcome.Retry.class, r -> assertThat(r.error()).isSameAs(error));
        assertThat(held.get(1)).as("the other key's record; the skipped one is not reported")
                .isEqualTo(Outcome.delivered());
        assertThat(reported).hasSize(3);
        assertThat(reported.get(0)).isInstanceOfSatisfying(Outcome.Permanent.class,
                p -> assertThat(p.error()).isSameAs(error));
    }

    private static OutboxRecord record(long id, String key, String payload) {
        return new OutboxRecord(id, UUID.randomUUID(), "t", key.getBytes(UTF_8), payload.getBytes(UTF_8), List.of(),
                Instant.now(), 0, 0);
    }
}
