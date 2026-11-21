package io.github.dsudomoin.outbox.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.dsudomoin.outbox.OutboxGauge;
import io.github.dsudomoin.outbox.OutboxHeader;
import io.github.dsudomoin.outbox.OutboxMessage;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTelemetry;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.testing.CapturedStderr;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleSupplier;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

class GuardedTelemetryTest {

    private static final List<OutboxHeader> HEADERS = List.of(new OutboxHeader("a", new byte[] {1}));
    private static final OutboxRecord RECORD =
            new OutboxRecord(1, UUID.randomUUID(), "orders", null, null, HEADERS, Instant.EPOCH, 0, 0);
    private static final List<OutboxRecord> RECORDS = List.of(RECORD);
    private static final OutboxMessage MESSAGE = OutboxMessage.of("orders", new byte[] {2});
    private static final UUID MESSAGE_ID = UUID.randomUUID();

    @Test
    void throwingEnqueueLeavesHeadersUnchanged() {
        var enqueue = GuardedTelemetry.of(new Throwing()).enqueueStarted();
        assertThat(enqueue.headers(HEADERS)).isSameAs(HEADERS);
        assertThatCode(enqueue::finished).doesNotThrowAnyException();
        assertThatCode(() -> enqueue.failed(new IOException("x"))).doesNotThrowAnyException();

        var failingStart = GuardedTelemetry.of(new ThrowingStart()).enqueueStarted();
        assertThat(failingStart.headers(HEADERS)).isSameAs(HEADERS);
    }

    @Test
    void throwingDeliveryHandsRecordsUnchanged() {
        var delivery = GuardedTelemetry.of(new Throwing())
                .deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);
        assertThat(delivery.handed(RECORDS)).isSameAs(RECORDS);
        assertThatCode(() -> delivery.finished(List.of(Outcome.delivered()))).doesNotThrowAnyException();
        assertThatCode(() -> delivery.failed(new IOException("x"))).doesNotThrowAnyException();

        var failingStart = GuardedTelemetry.of(new ThrowingStart())
                .deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);
        assertThat(failingStart.handed(RECORDS)).isSameAs(RECORDS);
    }

    @Test
    void handedListOfAnotherSizeIsIgnored() {
        var shrinking = new OutboxTelemetry() {
            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                return new Delivery() {
                    @Override
                    public List<OutboxRecord> handed(List<OutboxRecord> records) {
                        return List.of();
                    }
                };
            }
        };
        var delivery = GuardedTelemetry.of(shrinking)
                .deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);
        assertThat(delivery.handed(RECORDS)).isSameAs(RECORDS);
    }

    @Test
    void throwingEventIsSwallowed() {
        var t = GuardedTelemetry.of(new Throwing());
        assertThatCode(() -> {
            t.enqueued("orders");
            t.delivered("orders", Duration.ofMillis(1));
            t.retried("orders");
            t.requeued("orders");
            t.waited("orders", Outcome.Waiting.Kind.CONTRACT);
            t.contractRejected("orders", "Created", 2);
            t.routeRejected("orders");
            t.routeUnchecked("orders");
            t.stepFinished(Duration.ofMillis(1));
            t.shardPaused();
            t.deadLetterDecision("keep");
            t.deadLetterHookFailed();
            t.inboxOutsideWindow();
            t.releaseThread();
            t.gauge(OutboxGauge.SHARD_LAG, () -> 1);
            t.queueState(List.of(new OutboxTelemetry.ShardQueue(0, 1, 2, false)));
        }).doesNotThrowAnyException();
    }

    @Test
    void eventsAreForwarded() {
        var events = new ArrayList<String>();
        var t = GuardedTelemetry.of(new OutboxTelemetry() {
            @Override
            public void waited(String topic, Outcome.Waiting.Kind kind) {
                events.add("waited " + topic + " " + kind);
            }

            @Override
            public void contractRejected(String topic, String type, int version) {
                events.add("rejected " + topic + " " + type + " " + version);
            }

            @Override
            public void routeRejected(String topic) {
                events.add("routeRejected " + topic);
            }

            @Override
            public void routeUnchecked(String topic) {
                events.add("routeUnchecked " + topic);
            }
        });

        t.waited("orders", Outcome.Waiting.Kind.NO_ROUTE);
        t.contractRejected("orders", "Created", 2);
        t.routeRejected("orders");
        t.routeUnchecked("audit");

        assertThat(events).containsExactly("waited orders NO_ROUTE", "rejected orders Created 2",
                "routeRejected orders", "routeUnchecked audit");
    }

    @Test
    void throwingRelayHooksAreIgnored() {
        assertThat(GuardedTelemetry.of(new ThrowingStart()).stepStarted(List.of(1)))
                .isSameAs(OutboxTelemetry.RelayStep.NONE);

        var t = GuardedTelemetry.of(new Throwing());
        var step = t.stepStarted(List.of(1));
        assertThatCode(step::finished).doesNotThrowAnyException();
        assertThatCode(() -> step.failed(new IOException("x"))).doesNotThrowAnyException();
        assertThatCode(() -> t.relayFailed(new IOException("x"))).doesNotThrowAnyException();
    }

    @Test
    void throwingStepSettleAndRecordHooksAreLoggedAndIgnored() {
        try (var err = new CapturedStderr()) {
            var t = GuardedTelemetry.of(new Throwing());
            var step = t.stepStarted(List.of(1));
            assertThatCode(() -> step.fetched(1, 3)).doesNotThrowAnyException();
            var settle = step.settleStarted();
            assertThatCode(() -> settle.committed(2)).doesNotThrowAnyException();
            assertThatCode(() -> settle.failed(2, new IOException("x"))).doesNotThrowAnyException();
            var delivery = t.deliveryStarted(step, RECORDS, Instant.EPOCH);
            assertThatCode(() -> delivery.recordFinished(RECORD, Outcome.delivered())).doesNotThrowAnyException();
            assertThatCode(() -> t.deadLettered(RECORD, "max_attempts")).doesNotThrowAnyException();

            var text = err.text();
            for (var hook : List.of("RelayStep.fetched", "Settle.committed", "Settle.failed",
                    "Delivery.recordFinished", "deadLettered")) {
                assertThat(occurrences(text, "hook " + hook + " failed")).as(hook).isEqualTo(1);
            }
        }
    }

    @Test
    void throwingSettleStartAndStepDeliveryStartAreIgnored() {
        try (var err = new CapturedStderr()) {
            var t = GuardedTelemetry.of(new ThrowingStart());
            var step = new OutboxTelemetry.RelayStep() {
                @Override
                public OutboxTelemetry.Settle settleStarted() {
                    throw new IllegalStateException("telemetry bug");
                }
            };
            var guarded = GuardedTelemetry.of(new OutboxTelemetry() {
                @Override
                public RelayStep stepStarted(List<Integer> shards) {
                    return step;
                }
            });
            assertThat(guarded.stepStarted(List.of(1)).settleStarted()).isSameAs(OutboxTelemetry.Settle.NONE);
            assertThat(t.deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH))
                    .isSameAs(OutboxTelemetry.Delivery.NONE);
            var text = err.text();
            assertThat(occurrences(text, "hook RelayStep.settleStarted failed")).isEqualTo(1);
            assertThat(occurrences(text, "hook deliveryStarted failed")).isEqualTo(1);
        }
    }

    @Test
    void deliveryStartedGetsTheDelegatesOwnStep() {
        var own = new OutboxTelemetry.RelayStep() {};
        var seen = new ArrayList<OutboxTelemetry.RelayStep>();
        var t = GuardedTelemetry.of(new OutboxTelemetry() {
            @Override
            public RelayStep stepStarted(List<Integer> shards) {
                return own;
            }

            @Override
            public Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, Instant now) {
                seen.add(step);
                return Delivery.NONE;
            }
        });

        var step = t.stepStarted(List.of(1));
        t.deliveryStarted(step, RECORDS, Instant.EPOCH);
        t.deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);

        assertThat(step).isNotSameAs(own);
        assertThat(seen).containsExactly(own, OutboxTelemetry.RelayStep.NONE);
    }

    @Test
    void aNullStepIsNone() {
        var seen = new ArrayList<OutboxTelemetry.RelayStep>();
        var t = GuardedTelemetry.of(new OutboxTelemetry() {
            @Override
            public RelayStep stepStarted(List<Integer> shards) {
                return null;
            }

            @Override
            public Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, Instant now) {
                seen.add(step);
                return Delivery.NONE;
            }
        });

        var step = t.stepStarted(List.of(1));
        t.deliveryStarted(step, RECORDS, Instant.EPOCH);

        assertThat(step).isSameAs(OutboxTelemetry.RelayStep.NONE);
        assertThat(seen).containsExactly(OutboxTelemetry.RelayStep.NONE);
    }

    @Test
    void newHooksDefaultToTheOldOnes() {
        var events = new ArrayList<String>();
        var t = GuardedTelemetry.of(new OutboxTelemetry() {
            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                events.add("deliveryStarted " + records.size());
                return Delivery.NONE;
            }

            @Override
            public void deadLettered(String topic) {
                events.add("deadLettered " + topic);
            }
        });

        t.deliveryStarted(t.stepStarted(List.of(1)), RECORDS, Instant.EPOCH);
        t.deadLettered(RECORD, "requeue_limit");

        assertThat(events).containsExactly("deliveryStarted 1", "deadLettered orders");
        assertThat(t.stepStarted(List.of(1))).isSameAs(OutboxTelemetry.RelayStep.NONE);
        assertThat(OutboxTelemetry.RelayStep.NONE.settleStarted()).isSameAs(OutboxTelemetry.Settle.NONE);
    }

    @Test
    void callExceptionsPassThrough() throws Exception {
        var plain = new OutboxTelemetry() {
            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                return new Delivery() {};
            }
        };
        var delivery = GuardedTelemetry.of(plain)
                .deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);
        var failure = new IllegalStateException("handler");
        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> delivery.inRecordScope(RECORD, () -> {
                throw failure;
            })).isSameAs(failure);
            assertThat(err.text()).doesNotContain("WARN");
        }
    }

    @Test
    void scopeThatSwallowsTheCallsFailureStillFails() {
        var swallowing = new OutboxTelemetry() {
            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                return new Delivery() {
                    @Override
                    public <T> T inRecordScope(OutboxRecord record, Callable<T> call) {
                        try {
                            return call.call();
                        } catch (Exception e) {
                            return null; // a bug: the handler's failure would read as a delivery
                        }
                    }
                };
            }
        };
        var delivery = GuardedTelemetry.of(swallowing)
                .deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);
        var failure = new IOException("handler");
        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> delivery.inRecordScope(RECORD, () -> {
                throw failure;
            })).isSameAs(failure);
            assertThat(err.text()).contains("WARN").contains("Delivery.inRecordScope");
        }
    }

    @Test
    void callRunsOnceWhenScopeThrowsAfterOrBeforeIt() throws Exception {
        for (boolean after : new boolean[] {true, false}) {
            var calls = new AtomicInteger();
            var delivery = GuardedTelemetry.of(new ScopeThrowing(after))
                    .deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);
            assertThat(delivery.inRecordScope(RECORD, calls::incrementAndGet)).as("after " + after).isEqualTo(1);
            assertThat(calls).as("after " + after).hasValue(1);
        }
    }

    @Test
    void callRunsOnceWhenScopeCallsItTwice() throws Exception {
        var calls = new AtomicInteger();
        var twice = new OutboxTelemetry() {
            @Override
            public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
                return new Delivery() {
                    @Override
                    public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
                        call.call();
                        return call.call();
                    }
                };
            }
        };
        var delivery = GuardedTelemetry.of(twice)
                .deliveryStarted(OutboxTelemetry.RelayStep.NONE, RECORDS, Instant.EPOCH);
        try (var err = new CapturedStderr()) {
            assertThat(delivery.inRecordScope(RECORD, calls::incrementAndGet)).isEqualTo(1);
            assertThat(err.text()).contains("Delivery.inRecordScope");
        }
        assertThat(calls).as("a second run would hand the record to its target twice").hasValue(1);
    }

    @Test
    void throwingMessageIsIgnored() {
        try (var err = new CapturedStderr()) {
            var message = GuardedTelemetry.of(new Throwing()).enqueueStarted().message(MESSAGE, MESSAGE_ID, 3);
            assertThat(message).isSameAs(OutboxTelemetry.Message.NONE);
            assertThat(occurrences(err.text(), "hook Enqueue.message failed")).isEqualTo(1);
        }
    }

    @Test
    void messageNoneIsNotWrapped() {
        var plain = new OutboxTelemetry() {
            @Override
            public Enqueue enqueueStarted() {
                return new Enqueue() {};
            }
        };
        var enqueue = GuardedTelemetry.of(plain).enqueueStarted();
        assertThat(enqueue.message(MESSAGE, MESSAGE_ID, 3)).isSameAs(OutboxTelemetry.Message.NONE);
    }

    @Test
    void messageGetsItsIdAndShard() {
        var seen = new ArrayList<String>();
        var t = GuardedTelemetry.of(new OutboxTelemetry() {
            @Override
            public Enqueue enqueueStarted() {
                return new Enqueue() {
                    @Override
                    public Message message(OutboxMessage message, UUID recordId, int shard) {
                        seen.add(message.topic() + " " + recordId + " " + shard);
                        return null;
                    }
                };
            }
        });

        var message = t.enqueueStarted().message(MESSAGE, MESSAGE_ID, 3);

        assertThat(seen).containsExactly("orders " + MESSAGE_ID + " 3");
        assertThat(message).as("null reads as none").isSameAs(OutboxTelemetry.Message.NONE);
    }

    @Test
    void throwingMessageHooksAreLoggedOnceAndIgnored() throws Exception {
        var calls = new AtomicInteger();
        try (var err = new CapturedStderr()) {
            var message = GuardedTelemetry.of(new MessageThrowing()).enqueueStarted().message(MESSAGE, MESSAGE_ID, 3);
            for (int i = 1; i <= 2; i++) {
                assertThat(message.headers(HEADERS)).isSameAs(HEADERS);
                assertThat(message.inScope(calls::incrementAndGet)).as("the insert's own result").isEqualTo(i);
                assertThatCode(message::inserted).doesNotThrowAnyException();
                assertThatCode(() -> message.failed(new IOException("x"))).doesNotThrowAnyException();
            }
            assertThat(calls).as("the insert ran once per scope, though the scope threw before it").hasValue(2);
            var text = err.text();
            for (var hook : List.of("Message.headers", "Message.inScope", "Message.inserted", "Message.failed")) {
                assertThat(occurrences(text, "hook " + hook + " failed")).as(hook).isEqualTo(1);
            }
        }
    }

    @Test
    void insertRunsOnceWhenMessageScopeThrowsAfterItOrSkipsIt() throws Exception {
        for (var scope : List.of(Scope.THROW_AFTER, Scope.SKIP)) {
            var calls = new AtomicInteger();
            var message = GuardedTelemetry.of(new MessageScope(scope)).enqueueStarted()
                    .message(MESSAGE, MESSAGE_ID, 3);
            assertThat(message.inScope(calls::incrementAndGet)).as(scope.toString()).isEqualTo(1);
            assertThat(calls).as(scope.toString()).hasValue(1);
        }
    }

    @Test
    void insertRunsOnceWhenMessageScopeCallsItTwice() throws Exception {
        var calls = new AtomicInteger();
        var message = GuardedTelemetry.of(new MessageScope(Scope.TWICE)).enqueueStarted()
                .message(MESSAGE, MESSAGE_ID, 3);
        try (var err = new CapturedStderr()) {
            assertThat(message.inScope(calls::incrementAndGet)).isEqualTo(1);
            assertThat(err.text()).contains("Message.inScope");
        }
        assertThat(calls).as("a second insert would write the message twice").hasValue(1);
    }

    @Test
    void insertFailurePassesThroughTheMessageScope() {
        var message = GuardedTelemetry.of(new MessageScope(Scope.SWALLOW)).enqueueStarted()
                .message(MESSAGE, MESSAGE_ID, 3);
        var failure = new IllegalStateException("insert");
        try (var err = new CapturedStderr()) {
            assertThatThrownBy(() -> message.inScope(() -> {
                throw failure;
            })).isSameAs(failure);
            assertThat(err.text()).contains("Message.inScope");
        }
    }

    @Test
    void warnsAtMostOncePer10s() {
        var t = GuardedTelemetry.of(new Throwing());
        try (var err = new CapturedStderr()) {
            t.enqueued("orders");
            t.enqueued("orders");
            var text = err.text();
            assertThat(text.split("WARN", -1)).as("one WARN within 10 s").hasSize(2);
            assertThat(text).contains("enqueued");
        }
    }

    @Test
    void noneIsNotWrapped() {
        assertThat(GuardedTelemetry.of(OutboxTelemetry.NONE)).isSameAs(OutboxTelemetry.NONE);
    }

    @Test
    void guardedIsNotWrappedAgain() {
        var guarded = GuardedTelemetry.of(new Throwing());
        assertThat(GuardedTelemetry.of(guarded)).isSameAs(guarded);
    }

    /**
     * Every hook throws; its enqueue, delivery and relay step throw from every method, their scopes before running the
     * call.
     */
    private static class Throwing implements OutboxTelemetry {

        private static RuntimeException bug() {
            return new IllegalStateException("telemetry bug");
        }

        @Override
        public Enqueue enqueueStarted() {
            return new Enqueue() {
                @Override
                public List<OutboxHeader> headers(List<OutboxHeader> headers) {
                    throw bug();
                }

                @Override
                public Message message(OutboxMessage message, UUID recordId, int shard) {
                    throw bug();
                }

                @Override
                public void finished() {
                    throw bug();
                }

                @Override
                public void failed(Throwable error) {
                    throw bug();
                }
            };
        }

        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            return new Delivery() {
                @Override
                public List<OutboxRecord> handed(List<OutboxRecord> records) {
                    throw bug();
                }

                @Override
                public <T> T inRecordScope(OutboxRecord record, Callable<T> call) {
                    throw bug();
                }

                @Override
                public void recordFinished(OutboxRecord record, Outcome outcome) {
                    throw bug();
                }

                @Override
                public void finished(List<Outcome> outcomes) {
                    throw bug();
                }

                @Override
                public void failed(Throwable error) {
                    throw bug();
                }
            };
        }

        @Override
        public RelayStep stepStarted(List<Integer> shards) {
            return new RelayStep() {
                @Override
                public void fetched(int shard, int records) {
                    throw bug();
                }

                @Override
                public Settle settleStarted() {
                    return new Settle() {
                        @Override
                        public void committed(int records) {
                            throw bug();
                        }

                        @Override
                        public void failed(int records, Throwable error) {
                            throw bug();
                        }
                    };
                }

                @Override
                public void finished() {
                    throw bug();
                }

                @Override
                public void failed(Throwable error) {
                    throw bug();
                }
            };
        }

        @Override
        public void relayFailed(Throwable error) {
            throw bug();
        }

        @Override
        public void enqueued(String topic) {
            throw bug();
        }

        @Override
        public void delivered(String topic, Duration delay) {
            throw bug();
        }

        @Override
        public void retried(String topic) {
            throw bug();
        }

        @Override
        public void deadLettered(String topic) {
            throw bug();
        }

        @Override
        public void deadLettered(OutboxRecord record, String errorType) {
            throw bug();
        }

        @Override
        public void requeued(String topic) {
            throw bug();
        }

        @Override
        public void waited(String topic, Outcome.Waiting.Kind kind) {
            throw bug();
        }

        @Override
        public void contractRejected(String topic, String type, int version) {
            throw bug();
        }

        @Override
        public void routeRejected(String topic) {
            throw bug();
        }

        @Override
        public void routeUnchecked(String topic) {
            throw bug();
        }

        @Override
        public void stepFinished(Duration duration) {
            throw bug();
        }

        @Override
        public void shardPaused() {
            throw bug();
        }

        @Override
        public void deadLetterDecision(String decision) {
            throw bug();
        }

        @Override
        public void deadLetterHookFailed() {
            throw bug();
        }

        @Override
        public void inboxOutsideWindow() {
            throw bug();
        }

        @Override
        public void releaseThread() {
            throw bug();
        }

        @Override
        public void gauge(OutboxGauge gauge, DoubleSupplier value) {
            throw bug();
        }

        @Override
        public void queueState(List<ShardQueue> shards) {
            throw bug();
        }
    }

    private static final class ThrowingStart implements OutboxTelemetry {
        @Override
        public Enqueue enqueueStarted() {
            throw new IllegalStateException("telemetry bug");
        }

        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            throw new IllegalStateException("telemetry bug");
        }

        @Override
        public Delivery deliveryStarted(RelayStep step, List<OutboxRecord> records, Instant now) {
            throw new IllegalStateException("telemetry bug");
        }

        @Override
        public RelayStep stepStarted(List<Integer> shards) {
            throw new IllegalStateException("telemetry bug");
        }
    }

    /** A record scope that throws after running its call ({@code after}) or instead of running it. */
    private record ScopeThrowing(boolean after) implements OutboxTelemetry {
        @Override
        public Delivery deliveryStarted(List<OutboxRecord> records, Instant now) {
            return new Delivery() {
                @Override
                public <T> T inRecordScope(OutboxRecord record, Callable<T> call) throws Exception {
                    if (after) {
                        call.call();
                    }
                    throw new IllegalStateException("telemetry bug");
                }
            };
        }
    }
    /** Every message hook throws, its scope before running the insert. */
    private static final class MessageThrowing implements OutboxTelemetry {
        @Override
        public Enqueue enqueueStarted() {
            return new Enqueue() {
                @Override
                public Message message(OutboxMessage message, UUID recordId, int shard) {
                    return new Message() {
                        @Override
                        public List<OutboxHeader> headers(List<OutboxHeader> headers) {
                            throw new IllegalStateException("telemetry bug");
                        }

                        @Override
                        public <T> T inScope(Callable<T> insert) {
                            throw new IllegalStateException("telemetry bug");
                        }

                        @Override
                        public void inserted() {
                            throw new IllegalStateException("telemetry bug");
                        }

                        @Override
                        public void failed(Throwable error) {
                            throw new IllegalStateException("telemetry bug");
                        }
                    };
                }
            };
        }
    }

    private enum Scope { THROW_AFTER, SKIP, TWICE, SWALLOW }

    /** A message scope with one of the bugs of {@link Scope}. */
    private record MessageScope(Scope bug) implements OutboxTelemetry {
        @Override
        public Enqueue enqueueStarted() {
            return new Enqueue() {
                @Override
                public Message message(OutboxMessage message, UUID recordId, int shard) {
                    return new Message() {
                        @Override
                        public <T> T inScope(Callable<T> insert) throws Exception {
                            switch (bug) {
                                case THROW_AFTER -> {
                                    insert.call();
                                    throw new IllegalStateException("telemetry bug");
                                }
                                case SKIP -> {
                                    return null;
                                }
                                case TWICE -> {
                                    insert.call();
                                    return insert.call();
                                }
                                default -> {
                                    try {
                                        return insert.call();
                                    } catch (Exception e) {
                                        return null;
                                    }
                                }
                            }
                        }
                    };
                }
            };
        }
    }

    private static int occurrences(String text, String part) {
        return text.split(Pattern.quote(part), -1).length - 1;
    }
}
