package io.github.dsudomoin.outbox.kafka;

import io.github.dsudomoin.outbox.internal.Backoff;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A partition (or topic) whose sends failed is skipped for 1 s, doubling per consecutive failure up to
 * 30 s; a success resets it. When a cooldown ends, one caller probes the key (half-open): it holds the key for
 * {@code lease} while everyone else still sees it cooling, until the probe's success resets it, its failure doubles it,
 * or the lease ends and the next caller probes. While a probe runs, the others are told to come back after as long as
 * the probe has run so far, at least 1 s and at most until the lease ends: a probe that succeeds in milliseconds frees
 * the key within about a second, while a probe of a dead key (which takes the whole lease) has the others re-check
 * about five times in a 20 s lease (after 1, 2, 4, 8, 16 s), not every second.
 */
final class Cooldown<K> {

    private static final Duration INITIAL = Duration.ofSeconds(1);
    private static final Duration MAX = Duration.ofSeconds(30);

    /** {@code until} on the {@link System#nanoTime} scale: a step of the wall clock does not move it. */
    private record State(int failures, long until, boolean probing) {}

    private final Map<K, State> states = new ConcurrentHashMap<>();
    private final long lease;

    Cooldown(Duration lease) {
        this.lease = lease.toNanos();
    }

    /**
     * How long until the caller may come back, or null when {@code key} may be sent to at {@code now} (nanos). Null
     * for an ended cooldown leases the key to this caller: it must send to it, as its probe.
     */
    @Nullable
    Duration coolingUntil(K key, long now) {
        var state = states.get(key);
        if (state == null) {
            return null;
        }
        if (state.until() - now > 0) {
            return retryAt(state, now);
        }
        var probe = new boolean[1];
        state = states.computeIfPresent(key, (k, s) -> {
            if (s.until() - now > 0) {
                return s; // another caller took the lease
            }
            probe[0] = true;
            return new State(s.failures(), now + lease, true);
        });
        return state == null || probe[0] ? null : retryAt(state, now);
    }

    /** How long a caller that may not send should wait before coming back. */
    private Duration retryAt(State state, long now) {
        long left = state.until() - now;
        if (!state.probing()) {
            return Duration.ofNanos(left);
        }
        long probed = now - (state.until() - lease); // since the lease was taken
        return Duration.ofNanos(Math.min(Math.max(probed, INITIAL.toNanos()), left));
    }

    /** Like {@link #coolingUntil} but never leases: null when the cooldown has ended. */
    @Nullable
    Duration peek(K key, long now) {
        var state = states.get(key);
        return state != null && state.until() - now > 0 ? retryAt(state, now) : null;
    }

    /** Whether {@code key} failed and has not succeeded since, cooling or not. */
    boolean contains(K key) {
        return states.containsKey(key);
    }

    /** Returns the length of the new cooldown. */
    Duration failed(K key, long now) {
        return Duration.ofNanos(states.compute(key, (k, s) -> {
            int failures = s == null ? 0 : s.failures() + 1;
            return new State(failures, now + Backoff.delay(failures, INITIAL, MAX).toNanos(), false);
        }).until() - now);
    }

    void succeeded(K key) {
        states.remove(key);
    }
}
