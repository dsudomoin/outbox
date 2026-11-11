package io.github.dsudomoin.outbox;

import java.time.Instant;
import java.util.Objects;

/** What becomes of one dead letter, returned by {@link OutboxDeadLetterHandler#handle}. */
public sealed interface DeadLetterDecision {

    /** The letter stays in the dead table for humans; the hook is not called for it again. */
    static DeadLetterDecision keep() {
        return new Keep();
    }

    /** The letter is deleted. */
    static DeadLetterDecision discard() {
        return new Discard();
    }

    /**
     * The letter goes back into the outbox right after the decision commits, as by {@code redriveAt(now)}: a new
     * message after the later messages of its (topic, key), with the same message id, topic, key, payload and headers,
     * attempts reset; the dead row is deleted in the same transaction. While a transaction sending its key is open, it
     * waits {@link DeadLetter.Status#SCHEDULED}, tried again every {@code outbox.relay.retry.initialDelay}. Should it
     * die again within {@code outbox.deadLetters.redriveHistory}, its {@link DeadLetter#redrives()} is one higher, so a
     * hook can stop a redrive loop.
     */
    static DeadLetterDecision redrive() {
        return new Redrive();
    }

    /**
     * The letter is kept ({@link DeadLetter.Status#SCHEDULED}) and redriven as by {@link #redrive()} once {@code at}
     * comes, without a further call; {@code at} is clamped to [now, now + 7 days]. By this JVM's clock, as
     * {@code Instant.now().plusSeconds(3)}: that delay, counted on the database's clock, however far off this one is.
     */
    static DeadLetterDecision redriveAt(Instant at) {
        return new RedriveAt(at);
    }

    /**
     * Not yet: the letter stays {@link DeadLetter.Status#PENDING} and is handed again after the backoff a failed call
     * gives it, {@link DeadLetter#handlerAttempts()} one higher and {@code error} as its last hook error, while the
     * decisions about its batch-mates commit. For a letter the hook could not act on now, so it does not fail the call
     * and hold the whole batch back with it.
     */
    static DeadLetterDecision retryLater(Throwable error) {
        return new RetryLater(error);
    }

    record Keep() implements DeadLetterDecision {}

    record Discard() implements DeadLetterDecision {}

    record Redrive() implements DeadLetterDecision {}

    record RedriveAt(Instant at) implements DeadLetterDecision {
        public RedriveAt {
            Objects.requireNonNull(at, "at");
        }
    }

    record RetryLater(Throwable error) implements DeadLetterDecision {
        public RetryLater {
            Objects.requireNonNull(error, "error");
        }
    }
}
