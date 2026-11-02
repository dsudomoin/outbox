package io.github.dsudomoin.outbox.internal;

import jakarta.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * The database's clock, by which a relay schedules and compares every persisted time ({@code paused_until},
 * {@code next_attempt_at}, {@code due_at}, {@code retry_due_at}, {@code parked_due_at}, {@code waiting_since}), so a JVM
 * clock off by any amount neither stalls another instance nor shortens a backoff. A statement the relay runs anyway
 * brings a reading ({@link Ticks#latest(java.sql.Connection, DatabaseClock)}: no round trip of its own), and
 * {@code nanoTime} counts on from it until the next, so a JVM clock stepped between two readings changes nothing.
 * Thread-safe; a later reading of another worker simply replaces the last. Message ids and the inbox's window follow it
 * too ({@link #millis}), as ids a clock off by more than the inbox's retention lie outside the window of dedup; there
 * the readings come from the heartbeat, the relay's ticks and the inbox's maintenance, never from a send or an accept.
 */
public final class DatabaseClock {

    /** Further from now than any bound on these times (24 h, 7 days), and short of {@link Instant}'s range. */
    private static final Duration HORIZON = Duration.ofDays(366);

    private record Reading(Instant at, long nanoTime) {}

    private final InstantSource jvm;
    @Nullable
    private volatile Reading reading;

    /** {@code jvm}: the clock handlers read, for {@link #fromJvm}. */
    public DatabaseClock(InstantSource jvm) {
        this.jvm = jvm;
    }

    void read(Instant databaseNow) {
        reading = new Reading(databaseNow, System.nanoTime());
    }

    /** A reading by a statement of its own: for a caller with no statement that can bring one. */
    public void read(Connection c) throws SQLException {
        try (var ps = c.prepareStatement("select clock_timestamp()"); var rs = ps.executeQuery()) {
            rs.next();
            read(rs.getObject(1, OffsetDateTime.class).toInstant());
        }
    }

    /** {@link #now()} in epoch milliseconds, with no allocation to speak of: on the path of every send and accept. */
    public long millis() {
        var r = reading;
        return r == null ? jvm.millis() : r.at().toEpochMilli() + (System.nanoTime() - r.nanoTime()) / 1_000_000;
    }

    /** Now by the last reading, in microseconds as Postgres stores it; by the JVM clock before the first reading. */
    public Instant now() {
        var r = reading;
        var now = r == null ? jvm.instant() : r.at().plusNanos(System.nanoTime() - r.nanoTime());
        return now.truncatedTo(ChronoUnit.MICROS);
    }

    /**
     * A time a handler gave by its own JVM clock ({@code Instant.now().plusSeconds(3)}) on the database's: as far from
     * now as it meant; beyond {@link #HORIZON}, that far, so {@link Instant#MAX} overflows nothing. As it is before the
     * first reading, where no other clock is known.
     */
    public Instant fromJvm(Instant jvmTime) {
        if (reading == null) {
            return jvmTime;
        }
        var jvmNow = jvm.instant();
        var ahead = jvmTime.isAfter(jvmNow.plus(HORIZON)) ? HORIZON
                : jvmTime.isBefore(jvmNow.minus(HORIZON)) ? HORIZON.negated() : Duration.between(jvmNow, jvmTime);
        return now().plus(ahead);
    }
}
